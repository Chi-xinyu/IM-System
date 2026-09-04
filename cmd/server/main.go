package main

import (
	"IM-System/internal/chatcore"
	"IM-System/internal/db"
	"IM-System/internal/redis"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// Client 单个连接
type Client struct {
	Nick     string          // 用户昵称
	Addr     string          // 远端地址
	ClientTp string          // web / tcp
	wsConn   *websocket.Conn // websocket连接，如果是tcp则为nil
	tcpConn  net.Conn        // tcp连接，如果是web则为nil
	sendChan chan []byte
}

// OnlineUser 返回给前端的结构体
type OnlineUser struct {
	Name       string `json:"name"`
	Addr       string `json:"addr"`
	ClientType string `json:"clientType"` // web / tcp
}

// Hub 全局聊天室管理器
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client // key:昵称
}

// WebSocket升级配置
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var bootTime = time.Now()
var tcpServer *chatcore.Server

// 全局实例 globalHub
var globalHub = &Hub{
	clients: make(map[string]*Client),
}

// GetOnlineCount 获取在线人数
func (h *Hub) GetOnlineCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// GetOnlineList 获取在线用户列表
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

// AddClient 添加用户 + 推送上线系统消息
func (h *Hub) AddClient(cli *Client) {
	h.mu.Lock()
	h.clients[cli.Nick] = cli
	h.mu.Unlock()
	msg := []byte("[系统]" + cli.Nick + " 进入聊天室")
	h.Broadcast(msg)
	// 入库系统通知
	_ = db.SaveMsg("[系统]", cli.Nick+" 进入聊天室")
}

// RemoveClient 删除用户 + 推送下线系统消息
func (h *Hub) RemoveClient(nick string) {
	h.mu.Lock()
	cli, ok := h.clients[nick]
	delete(h.clients, nick)
	h.mu.Unlock()
	if ok {
		msg := []byte("[系统]" + nick + " 离开聊天室")
		h.Broadcast(msg)
		_ = db.SaveMsg("[系统]", nick+" 离开聊天室")
		_ = cli.Close()
	}
}

// Close 关闭单个客户端
func (c *Client) Close() error {
	if c.wsConn != nil {
		_ = c.wsConn.Close()
	}
	if c.tcpConn != nil {
		_ = c.tcpConn.Close()
	}
	close(c.sendChan)
	globalHub.RemoveClient(c.Nick)
	return nil
}

// KickAll 全部踢出
func (h *Hub) KickAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, cli := range h.clients {
		cli.Close()
	}
	h.clients = make(map[string]*Client)
}

// Broadcast 全局广播
func (h *Hub) Broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, cli := range h.clients {
		select {
		case cli.sendChan <- msg:
		default:
			// 通道满，丢弃，防止阻塞
		}
	}
}

// writeLoop 独立发送协程，保证串行写，杜绝并发panic
func (c *Client) writeLoop() {
	for msg := range c.sendChan {
		if c.wsConn != nil {
			err := c.wsConn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				globalHub.RemoveClient(c.Nick)
				return
			}
		} else if c.tcpConn != nil {
			_, err := c.tcpConn.Write(append(msg, '\n'))
			if err != nil {
				globalHub.RemoveClient(c.Nick)
				return
			}
		}
	}
}

// wsHandler 标准WebSocket处理
func wsHandler(c *gin.Context) {
	// 从url参数获取昵称 chat.html?nick=xxx
	nick := c.Query("nick")
	if nick == "" {
		c.String(400, "缺少昵称参数")
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("upgrade err:", err)
		return
	}

	// 创建客户端实例
	cli := &Client{
		Nick:     nick,
		Addr:     c.ClientIP(),
		ClientTp: "web",
		wsConn:   conn,
		sendChan: make(chan []byte, 64),
	}

	globalHub.AddClient(cli)
	// 启动发送协程
	go cli.writeLoop()

	// 接收消息循环
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			globalHub.RemoveClient(cli.Nick)
			return
		}
		content := string(data)
		fullMsg := "[" + nick + "]" + content
		// 持久化消息
		_ = db.SaveMsg(nick, content)
		// 广播给所有web/tcp客户端
		globalHub.Broadcast([]byte(fullMsg))
		// 同步转发到TCP服务
		tcpServer.BroadCast(&chatcore.User{Name: nick}, content)
	}
}

// handleTCPConn 对接tcp服务，统一接入Hub
func handleTCPConn(conn net.Conn) {
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		_ = conn.Close()
		return
	}
	nick := string(buf[:n])

	client := &Client{
		Nick:     nick,
		Addr:     conn.RemoteAddr().String(),
		ClientTp: "tcp",
		tcpConn:  conn,
		sendChan: make(chan []byte, 64),
	}
	globalHub.AddClient(client)
	go client.writeLoop()
}

func main() {
	errRedis := redis.InitRedis()
	if errRedis != nil {
		panic("Redis初始化失败：" + errRedis.Error())
	}
	// 初始化SQLite数据库，创建消息/用户表
	errSql := db.InitDB()
	if errSql != nil {
		panic("数据库初始化失败：" + errSql.Error())
	}
	//启动TCP聊天室协程
	tcpServer := chatcore.NewServer("127.0.0.1", 8888)
	go tcpServer.Start()

	//初始化Gin
	r := gin.New()
	// 仅注册崩溃恢复，不会提前操作响应流
	r.Use(gin.Recovery())
	//托管前端静态文件夹web
	r.Static("/web", "./web")

	//接口分组
	api := r.Group("/api")
	{
		//获取在线用户
		api.GET("/online", func(c *gin.Context) {
			list := globalHub.GetOnlineList()
			c.JSON(200, gin.H{"code": 0, "data": list})
		})

		//踢出指定成员
		api.POST("/kick", func(c *gin.Context) {
			type KickReq struct {
				Username string `json:"Username"`
			}
			var req KickReq
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(200, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			globalHub.mu.RLock()
			target, ok := globalHub.clients[req.Username]
			globalHub.mu.RUnlock()
			if !ok {
				c.JSON(200, gin.H{"code": -1, "msg": "用户不存在"})
				return
			}
			_ = target.Close()
			globalHub.RemoveClient(req.Username)
			c.JSON(200, gin.H{"code": 0, "msg": "踢出成功"})
		})

		// 全局广播公告
		api.POST("/broadcast", func(c *gin.Context) {
			type BroadcastReq struct {
				Msg string `json:"msg"`
			}
			var req BroadcastReq
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(200, gin.H{"code": -1, "msg": "参数错误"})
				return
			}
			// 广播给所有在线客户端
			globalHub.Broadcast([]byte("[系统公告]" + req.Msg))
			c.JSON(200, gin.H{"code": 0, "msg": "广播发送成功"})
		})

		// 一键断开全部
		api.POST("/kickall", func(c *gin.Context) {
			globalHub.KickAll()
			c.JSON(200, gin.H{"code": 0, "msg": "已断开全部连接"})
		})

		// 服务状态
		api.GET("/status", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"code": 0,
				"data": gin.H{
					"onlineNum": globalHub.GetOnlineCount(),
					"bootTime":  bootTime.Format("2006-01-02 15:04:05"),
				},
			})
		})

		// WebSocket入口，统一使用wsHandler
		api.GET("/ws", wsHandler)

		// 历史消息
		api.GET("/history", func(c *gin.Context) {
			// 调用db包方法查询数据库
			msgList, err := db.GetHistoryMsg()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{
					"code": 500,
					"msg":  "查询数据库失败",
				})
				return
			}
			// 将数据库查询结果返回给前端
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": msgList,
			})
		})

		//启动Gin 8080端口
		err2 := r.Run(":8080")
		if err2 != nil {
			panic("Gin启动失败: " + err2.Error())
		}
	}
}
