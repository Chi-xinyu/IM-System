package main

import (
	"IM-System/internal/chatcore"
	"IM-System/internal/db"
	"IM-System/internal/redis"
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Client 单个 WebSocket 连接对象
//
// 并发安全设计（本次重构重点）：
//   - sendChan：发送缓冲通道，writeLoop 协程独占读取，保证串行写连接，杜绝并发写panic
//   - closeOnce + closed(atomic) + closeMu(RWMutex)：
//     任意路径（读循环错误/写循环错误/可能被……踢触发关闭，
//     closeOnce 保证 sendChan 只被 close 一次（重复 close 会 panic）；
//     TrySend 持读锁、Close 持写锁，保证"发送"与"关闭通道"互斥，
//     从根本上避免向已关闭 channel 发送导致的 panic。
//
// ---------------------------------------------------------------------------
type Client struct {
	Nick     string          // 用户昵称
	Addr     string          // 远端地址
	ClientTp string          // 客户端类型：web / tcp（当前 hub 仅管理 web）
	wsConn   *websocket.Conn // websocket连接，如果是tcp则为nil
	tcpConn  net.Conn        // tcp连接，如果是web则为nil（保留字段，便于将来扩展）
	sendChan chan []byte

	closeOnce sync.Once    // 保证 Close 幂等，只执行一次
	closeMu   sync.RWMutex // 发送/关闭互斥锁
	closed    atomic.Bool  // 关闭标记，供 TrySend 快速判断
}

// OnlineUser 返回给前端的在线用户结构体
type OnlineUser struct {
	Name       string `json:"name"`
	Addr       string `json:"addr"`
	ClientType string `json:"clientType"` // web / tcp
}

// ---------------------------------------------------------------------------
// Hub 全局聊天室管理器：管理所有 Web 端在线连接
// （TCP 端连接由 chatcore.Server 独立管理，两者通过 Redis 广播互通）
// ---------------------------------------------------------------------------
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client // key:昵称
}

// WebSocket升级配置：允许跨域（开发环境全放行）
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
	// 限制单个控制帧大小，防内存攻击
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// 服务启动时间（/api/status 展示 uptime 用）
var bootTime = time.Now()

// 全局 TCP 服务实例。
// 【致命bug修复】原代码 main() 中用 `tcpServer := ...` 声明局部变量，
// 遮蔽了此处全局变量，导致全局 tcpServer 恒为 nil；
// wsHandler 中调用 tcpServer.BroadCast(...) 时触发空指针 panic。
// 现在 main() 中必须使用 `tcpServer = ...`（赋值而非 := 声明）。
var tcpServer *chatcore.Server

// 全局实例 globalHub
var globalHub = &Hub{
	clients: make(map[string]*Client),
}

// ---------------------------------------------------------------------------
// Hub 核心方法
// ---------------------------------------------------------------------------

// GetOnlineCount 获取在线人数
func (h *Hub) GetOnlineCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// GetOnlineList 获取在线用户列表（/api/online 接口数据源）
func (h *Hub) GetOnlineList() []OnlineUser {
	h.mu.RLock()
	defer h.mu.RUnlock()
	list := make([]OnlineUser, 0, len(h.clients))
	for _, cli := range h.clients {
		list = append(list, OnlineUser{
			Name:       cli.Nick,
			Addr:       cli.Addr,
			ClientType: cli.ClientTp,
		})
	}
	return list
}

// AddClient 添加用户（重名返回 false）：写 Redis 在线缓存 + 广播上线系统消息
// 返回 false 表示昵称已被占用，调用方应拒绝该连接
func (h *Hub) AddClient(cli *Client) bool {
	h.mu.Lock()
	// 重名校验：map 中已存在同名在线用户则拒绝
	if _, exists := h.clients[cli.Nick]; exists {
		h.mu.Unlock()
		return false
	}
	h.clients[cli.Nick] = cli
	h.mu.Unlock()

	// 写 Redis 在线缓存（web 用户也纳入全端在线统计，TCP 的 who/私聊可感知）
	chatcore.SetUserOnline(cli.Nick, cli.Addr)
	// 上线系统消息：统一分发（入库 + Redis），所有 web/tcp 用户可见
	chatcore.BroadcastMsg("系统", cli.Nick+" 进入聊天室")
	return true
}

// RemoveClient 删除用户（所有下线路径的唯一入口）：
// 广播下线系统消息 → 删 Redis 缓存 → 安全关闭连接与通道
// 注意：Close 内部不再调用 RemoveClient，避免"删除→关闭→再删除"的递归死锁
func (h *Hub) RemoveClient(nick string) {
	h.mu.Lock()
	cli, ok := h.clients[nick]
	delete(h.clients, nick)
	h.mu.Unlock()
	if ok {
		chatcore.BroadcastMsg("系统", nick+" 离开聊天室")
		chatcore.DelUserOnline(nick)
		cli.Close() // 幂等关闭
	}
}

// Close 安全关闭单个客户端（幂等）：
// closeOnce 保证 sendChan 只 close 一次；持写锁保证与 TrySend 互斥
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		defer c.closeMu.Unlock()
		c.closed.Store(true) // 先标记关闭，后续 TrySend 直接拒绝
		if c.wsConn != nil {
			_ = c.wsConn.Close()
		}
		if c.tcpConn != nil {
			_ = c.tcpConn.Close()
		}
		close(c.sendChan) // writeLoop 的 range 读到关闭后退出
	})
}

// TrySend 安全发送消息：持读锁 + closed 检查，与 Close 互斥，杜绝向已关闭channel发送panic
// 通道满时非阻塞丢弃（聊天室对瞬时高峰消息允许丢，防止拖垮广播协程）
func (c *Client) TrySend(msg []byte) bool {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	if c.closed.Load() {
		return false
	}
	select {
	case c.sendChan <- msg:
		return true
	default:
		return false
	}
}

// BroadcastMsg 广播给所有在线 web 客户端（web 广播回调由 chatcore 消费协程触发）
func (h *Hub) BroadcastMsg(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, cli := range h.clients {
		cli.TrySend(msg)
	}
}

// KickAll 全部踢出：
// 【死锁bug修复】原代码先 Lock 再逐个 cli.Close()，而 Close 内部走
// RemoveClient 又申请 Lock，RWMutex 不可重入 → 必然死锁。
// 现在：先在锁内快照昵称列表，释放锁后再逐个移除
func (h *Hub) KickAll() {
	h.mu.Lock()
	nicks := make([]string, 0, len(h.clients))
	for nick := range h.clients {
		nicks = append(nicks, nick)
	}
	h.mu.Unlock()

	for _, nick := range nicks {
		h.RemoveClient(nick)
	}
}

// writeLoop 独立发送协程，串行写 WebSocket/TCP，杜绝并发写 panic
func (c *Client) writeLoop() {
	for msg := range c.sendChan {
		if c.wsConn != nil {
			// 每次写入设置写超时，防止对端假死导致写阻塞
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				globalHub.RemoveClient(c.Nick)
				return
			}
		} else if c.tcpConn != nil {
			if _, err := c.tcpConn.Write(append(msg, '\n')); err != nil {
				globalHub.RemoveClient(c.Nick)
				return
			}
		}
	}
}

// pingLoop 心跳协程：每30秒发送 Ping 帧，浏览器自动回 Pong，
// 配合读超时判断连接是否存活，防止僵尸连接占资源
func (c *Client) pingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if c.wsConn == nil {
				return
			}
			c.closeMu.RLock()
			if c.closed.Load() {
				c.closeMu.RUnlock()
				return
			}
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := c.wsConn.WriteMessage(websocket.PingMessage, nil)
			c.closeMu.RUnlock()
			if err != nil {
				globalHub.RemoveClient(c.Nick)
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

// validNick 昵称合法性校验：1~20个字符（按rune计算），
// 禁止空白/控制字符及 HTML 特殊字符（防 XSS 注入 + 防止破坏消息格式 [昵称] 内容）
func validNick(nick string) bool {
	if l := utf8.RuneCountInString(nick); l < 1 || l > 20 {
		return false
	}
	for _, r := range nick {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
		switch r {
		// 以下字符会破坏 [sender] 消息格式或被浏览器当作HTML解析
		case '<', '>', '&', '"', '\'', '[', ']', '{', '}', '`', '\\', '/', ':', ';':
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// WebSocket 处理
// ---------------------------------------------------------------------------

// wsHandler WebSocket 连接处理：校验昵称 → 重名检测 → 升级 → 心跳 → 消息循环
// 前端只发送纯内容，不再拼 [昵称] 前缀（原来前后端各拼一次导致消息重复前缀）
func wsHandler(c *gin.Context) {
	// 1) 昵称校验（升级握手前，失败直接 HTTP 400）
	nick := strings.TrimSpace(c.Query("nick"))
	if !validNick(nick) {
		c.String(http.StatusBadRequest,
			"昵称不合法：1-20个字符，不能包含空格及特殊字符 < > & \" ' [ ] { } ` \\ / : ;")
		return
	}

	// 2) 升级为 WebSocket 连接
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("[ws] upgrade err:", err)
		return
	}

	// 3) 创建客户端并加入 Hub（内部再做一次重名校验，防并发同名竞态）
	cli := &Client{
		Nick:     nick,
		Addr:     c.ClientIP(),
		ClientTp: "web",
		wsConn:   conn,
		sendChan: make(chan []byte, 128),
	}
	if !globalHub.AddClient(cli) {
		_ = conn.WriteMessage(websocket.TextMessage, []byte("[系统] 昵称已被占用，连接即将关闭"))
		_ = conn.Close()
		return
	}

	// 4) 启动发送协程与心跳协程
	go cli.writeLoop()
	go cli.pingLoop()

	// 5) 心跳读超时：90秒内未收到 Pong（浏览器自动响应 Ping）则判定断线
	conn.SetReadLimit(8192) // 单条消息上限8KB（无返回值，直接调用）
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(90 * time.Second))
	})

	// 6) 接收消息循环
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			// 正常断开/超时/异常：统一走 RemoveClient 清理
			globalHub.RemoveClient(cli.Nick)
			return
		}
		content := strings.TrimSpace(string(data))
		if content == "" {
			continue // 空消息忽略
		}
		// 消息统一出口：入库 + 发布Redis。
		// 本实例的消费协程收到后回环分发（含发送者自己），
		// 前端根据 sender == 自己昵称 判断左右气泡，无需本地回显
		chatcore.BroadcastMsg(cli.Nick, content)
	}
}

// ---------------------------------------------------------------------------
// 程序入口
// ---------------------------------------------------------------------------

// getenv 读取环境变量，为空时返回默认值（支持 PORT / TCP_PORT 自定义端口）
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	// 1) 初始化 Redis（消息分发总线 + 在线缓存 + 离线消息）
	if err := redis.InitRedis(); err != nil {
		log.Fatalf("[main] Redis初始化失败：%v", err)
	}

	// 2) 初始化 SQLite 数据库（历史消息持久化）
	if err := db.InitDB(); err != nil {
		log.Fatalf("[main] 数据库初始化失败：%v", err)
	}

	// 3) 启动 TCP 聊天室服务（全局变量赋值！勿用 := 遮蔽）
	//    监听 0.0.0.0 供 Docker 端口映射与外部 TCP 客户端连接
	//    端口可用环境变量 TCP_PORT 覆盖（默认8888）
	tcpPort, _ := strconv.Atoi(getenv("TCP_PORT", "8888"))
	tcpServer = chatcore.NewServer("0.0.0.0", tcpPort)
	go tcpServer.Start()

	// 4) 注入 web 广播回调：
	//    chatcore 消费 Redis 消息后，除了分发给 TCP 用户，还会调用此回调
	//    把消息广播给所有 web 端在线用户 —— 打通 web/TCP 双向互通
	tcpServer.SetWebBroadcast(func(sender, msg string) {
		globalHub.BroadcastMsg([]byte("[" + sender + "] " + msg))
	})

	// 5) 初始化 Gin 路由
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery()) // 仅注册崩溃恢复

	// 托管前端静态文件（web 目录：login.html / chat.html / index.html）
	r.Static("/web", "./web")
	// 根路径重定向到登录页
	r.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/web/login.html")
	})

	api := r.Group("/api")
	{
		// 获取在线用户列表（web + tcp 全端）
		api.GET("/online", func(c *gin.Context) {
			// 1) web 端在线用户
			list := globalHub.GetOnlineList()
			// 2) 合并 TCP 端在线用户（chatcore.Server 管理）
			if tcpServer != nil {
				for name, u := range tcpServer.GetAllOnlineUsers() {
					list = append(list, OnlineUser{
						Name:       name,
						Addr:       u.Addr,
						ClientType: "tcp",
					})
				}
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "data": list})
		})

		// 踢出指定成员（web 与 TCP 用户均可踢）
		api.POST("/kick", func(c *gin.Context) {
			type KickReq struct {
				Username string `json:"Username"`
			}
			var req KickReq
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			// 先查 web 端
			globalHub.mu.RLock()
			target, ok := globalHub.clients[req.Username]
			globalHub.mu.RUnlock()
			if ok {
				// 先给被踢用户发送提示，再移除（内部广播下线 + 关闭连接）
				target.TrySend([]byte("[系统] 您已被管理员移出聊天室"))
				globalHub.RemoveClient(req.Username)
				c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "踢出成功"})
				return
			}
			// 再查 TCP 端
			if tcpServer != nil && tcpServer.KickUser(req.Username) {
				c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "踢出成功"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "用户不存在"})
		})

		// 全局广播公告（系统公告走统一分发，全端可见）
		api.POST("/broadcast", func(c *gin.Context) {
			type BroadcastReq struct {
				Msg string `json:"msg"`
			}
			var req BroadcastReq
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusOK, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			chatcore.BroadcastMsg("系统公告", req.Msg)
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "广播发送成功"})
		})

		// 一键断开全部连接
		api.POST("/kickall", func(c *gin.Context) {
			globalHub.KickAll()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "已断开全部连接"})
		})

		// 服务状态（在线人数/启动时间/运行时长/历史消息总数）
		api.GET("/status", func(c *gin.Context) {
			totalMsg, _ := db.GetMsgCount()
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": gin.H{
					"onlineNum": globalHub.GetOnlineCount(),
					"bootTime":  bootTime.Format("2006-01-02 15:04:05"),
					"uptimeSec": int(time.Since(bootTime).Seconds()),
					"totalMsg":  totalMsg,
				},
			})
		})

		// WebSocket 入口（统一处理）
		api.GET("/ws", wsHandler)

		// 历史消息（limit 参数控制条数，默认50，上限200）
		api.GET("/history", func(c *gin.Context) {
			limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
			msgList, err := db.GetHistoryMsgN(limit)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": "查询数据库失败"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "data": msgList})
		})
	}

	// 6) 启动 HTTP 服务（独立 goroutine，主协程等待退出信号做优雅关闭）
	//    端口可用环境变量 PORT 覆盖（默认8080），便于本地联调与部署到任意端口
	httpPort := getenv("PORT", "8080")
	srv := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("[main] HTTP服务启动成功: http://0.0.0.0:%s", httpPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] HTTP服务异常退出: %v", err)
		}
	}()

	// 7) 优雅关闭：等待 Ctrl+C / kill
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[main] 收到退出信号，开始优雅关闭...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx) // 停止接收新 HTTP 请求，等待存量请求完成
	globalHub.KickAll()   // 关闭所有 web 客户端连接
	tcpServer.Close()     // 关闭 TCP 服务（广播下线 + 释放连接）
	db.Flush()            // 等待异步入库队列清空，防止退出丢消息
	_ = redis.Close()     // 释放 Redis 连接池
	log.Println("[main] 服务已安全退出")
}
