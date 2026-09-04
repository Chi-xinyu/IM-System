package chatcore

import (
	"IM-System/internal/db"
	"IM-System/internal/redis"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net" // net：网络编程，TCP监听、连接相关API
	"strings"
	"sync" // sync：同步工具包，RWMutex读写互斥锁，解决map并发读写冲突
	"time"
)

// pubSubChannel Redis 分布式广播频道名：
// 所有实例（web网关/TCP服务）订阅同一频道，任何消息发布后所有实例都能收到并分发
const pubSubChannel = "im:broadcast"

// Server TCP 聊天室服务（同时承担"消息分发中枢"职责）
type Server struct {
	Ip        string
	Port      int
	OnlineMap map[string]*User // 在线用户列表（仅TCP客户端）
	mapLock   sync.RWMutex     // 读写互斥锁，保护OnlineMap并发增删查，防止多协程panic

	closeChan chan struct{}  // 服务关闭信号通道
	wg        sync.WaitGroup // 等待所有客户端Handler协程退出
	closeOnce sync.Once      // 保证 Close 只执行一次，防止重复 close(closeChan) panic

	sub *redis.PubSub // redis订阅器

	// webBroadcast 回调：由 web 网关（main.go）注入，
	// 当 Redis 消费到消息时，除了分发给本地 TCP 用户，还要通过该回调转发给 web 端用户
	webBroadcast func(sender, msg string)
}

// PubMsg 分布式广播消息体（Redis 频道中传输的 JSON 结构）
type PubMsg struct {
	Sender string `json:"sender"` // 发送者昵称（系统消息时为"系统"/"系统公告"）
	Msg    string `json:"msg"`    // 消息内容
}

// NewServer 创建 TCP 聊天室服务端，并启动 Redis 订阅消费协程
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		closeChan: make(chan struct{}),
	}
	// 订阅redis全局广播频道（订阅失败时 sub 为 nil，消费协程中做空判断）
	if redis.RedisClient != nil {
		server.sub = redis.Subscribe(pubSubChannel)
		// 启动redis消息消费协程：收到消息 → 分发本地TCP用户 + 转发web端
		go server.consumeRedisPubSub()
	}
	return server
}

// SetWebBroadcast 注入 web 端广播回调（main.go 启动时调用一次）
// 回调会在 consumeRedisPubSub 分发时被触发，参数为 (sender, msg)
func (server *Server) SetWebBroadcast(fn func(sender, msg string)) {
	server.webBroadcast = fn
}

// consumeRedisPubSub 消费 Redis 分布式广播，分发给本地 TCP 用户 + 转发 web 端
// 消息流：任何实例 BroadcastMsg → Redis Publish → 所有实例此处消费 → 本地分发
func (server *Server) consumeRedisPubSub() {
	if server.sub == nil {
		return
	}
	ch := server.sub.Channel() // go-redis 的 Channel() 返回只读消息channel
	for {
		select {
		case <-server.closeChan:
			// 服务关闭：取消订阅，释放连接
			_ = server.sub.Close()
			return
		case msg := <-ch:
			// 订阅被取消后 ch 会被关闭，读到的 msg 为 nil，直接退出避免死循环
			if msg == nil {
				return
			}
			var data PubMsg
			// 解析失败（消息体损坏）直接跳过，不影响后续消息
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				continue
			}
			if data.Sender == "" || data.Msg == "" {
				continue
			}
			// 统一消息格式：[发送者] 内容
			fullMsg := "[" + data.Sender + "] " + data.Msg
			// 1) 分发给本地所有 TCP 客户端（遍历副本，缩小锁持有时间）
			onlineCopy := server.GetAllOnlineUsers()
			for _, u := range onlineCopy {
				// 双保险：副本可能包含正在下线的用户，跳过避免无效投递
				if u.isClosed {
					continue
				}
				select {
				case u.C <- fullMsg:
				default:
					// 用户通道满，直接丢弃防止阻塞消费协程
				}
			}
			// 2) 通过回调转发给 web 端用户（由 main.go 注入的 Hub 广播实现）
			if server.webBroadcast != nil {
				server.webBroadcast(data.Sender, data.Msg)
			}
		}
	}
}

// AddUser 添加在线用户（登录流程：NewUser → AddUser → Online）
func (server *Server) AddUser(user *User) {
	server.mapLock.Lock()
	server.OnlineMap[user.Name] = user
	server.mapLock.Unlock()
}

// DelUser 删除在线用户
func (server *Server) DelUser(username string) {
	server.mapLock.Lock()
	delete(server.OnlineMap, username)
	server.mapLock.Unlock()
}

// GetAllOnlineUsers 获取全部在线用户（返回副本，避免外部持锁操作map）
func (server *Server) GetAllOnlineUsers() map[string]*User {
	copyMap := make(map[string]*User)
	server.mapLock.RLock()
	for k, v := range server.OnlineMap {
		copyMap[k] = v
	}
	server.mapLock.RUnlock()
	return copyMap
}

// GetUser 根据用户名查询用户（TCP 私聊时使用）
func (server *Server) GetUser(name string) (*User, bool) {
	server.mapLock.RLock()
	user, ok := server.OnlineMap[name]
	server.mapLock.RUnlock()
	return user, ok
}

// KickUser 管理员踢出指定 TCP 用户：发提示 → 强制下线（返回是否找到）
// 供 main.go 的 /api/kick 接口调用，与 web 端踢出逻辑对齐
func (server *Server) KickUser(name string) bool {
	user, ok := server.GetUser(name)
	if !ok {
		return false
	}
	user.SendMsg("[系统] 您已被管理员移出聊天室")
	user.Offline() // Offline 内部幂等：广播下线 + 删Redis缓存 + 关闭连接
	return true
}

// BroadcastMsg 消息统一出口（包级函数，web 端与 TCP 端共用）：
//  1. 消息入库（异步，不阻塞）
//  2. 发布到 Redis 频道
//
// 本地不直接分发 —— 由 consumeRedisPubSub 消费后统一分发，保证每个实例恰好收到一次，
// 既避免"本地广播 + Redis 广播"造成的重复消息，又天然支持多实例水平扩展
func BroadcastMsg(sender, content string) {
	// 入库（异步通道，不阻塞聊天主流程）
	if err := db.SaveMsg(sender, content); err != nil {
		log.Printf("[chatcore] 消息入库失败: %v", err)
	}
	// 发布到 Redis 频道，所有实例（含本实例）的消费协程会收到
	pubData, err := json.Marshal(PubMsg{Sender: sender, Msg: content})
	if err != nil {
		log.Printf("[chatcore] 消息序列化失败: %v", err)
		return
	}
	if err := redis.Publish(pubSubChannel, string(pubData)); err != nil {
		log.Printf("[chatcore] Redis发布失败: %v", err)
	}
}

// BroadCast TCP 客户端发消息的入口：统一走 BroadcastMsg
// user 参数仅用于取发送者昵称（web 端会构造临时 User{Name: nick} 传入）
func (server *Server) BroadCast(user *User, msg string) {
	if user == nil {
		return
	}
	BroadcastMsg(user.Name, msg)
}

// SetUserOnline 写入 Redis 在线用户缓存（web 端与 TCP 端上线时统一调用）
// 作用：who 指令查询在线列表、rename 重名校验、私聊目标在线判断都依赖它
func SetUserOnline(name, addr string) {
	info := UserInfo{Name: name, Addr: addr}
	data, _ := json.Marshal(info)
	_ = redis.SetKV(OnlineUserKeyPrefix+name, string(data), userTTL)
}

// DelUserOnline 删除 Redis 在线用户缓存（下线时统一调用）
func DelUserOnline(name string) {
	_ = redis.DelKV(OnlineUserKeyPrefix + name)
}

// IsUserOnline 查询某用户是否在线（依据 Redis 缓存，web/tcp 全端统一）
func IsUserOnline(name string) bool {
	_, err := redis.GetKV(OnlineUserKeyPrefix + name)
	return err == nil
}

// Handler 单个TCP连接处理协程：用户创建、上线、消息循环、超时保活、下线清理
func (server *Server) Handler(conn net.Conn) {
	server.wg.Add(1) // 计数 +1，Close 时会等待所有 Handler 退出
	defer server.wg.Done()

	// 开启 TCP keepalive，探测半开连接，防止僵尸连接占资源
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(60 * time.Second)
	}

	// 当前连接的业务：创建用户（默认昵称=对端地址）、上线
	user := NewUser(conn, server)
	server.AddUser(user)
	user.Online()

	// 监听用户是否活跃的channel（任意消息都会触发，用于重置超时踢人计时器）
	isLive := make(chan bool)
	// done：读协程退出信号。原代码读协程退出后 Handler 主循环仍阻塞，
	// 导致断线用户 2000 秒后才被清理（资源泄漏），此处补上退出通知
	done := make(chan struct{})

	// 接受客户端发送的消息
	go func() {
		defer close(done) // 读协程无论何种方式退出，都通知主循环
		buf := make([]byte, 1024*40)
		for {
			select {
			case <-server.closeChan:
				return // 服务关闭，读协程退出
			default:
			}
			n, err := conn.Read(buf)
			// 客户端主动关闭（n==0）或读到EOF：下线并结束
			if n == 0 {
				user.Offline()
				return
			}
			if err != nil && err != io.EOF {
				// 读取异常（连接重置等）：打印日志并下线
				log.Printf("[tcp] 客户端 %s 读取异常: %v", conn.RemoteAddr(), err)
				user.Offline()
				return
			}
			// 提取用户的消息：去掉末尾的 \n 与 \r（终端输入自带换行）
			msg := strings.TrimSuffix(string(buf[:n]), "\n")
			msg = strings.TrimSuffix(msg, "\r")
			// 处理用户消息（公聊/私聊/指令分发）
			user.DoMessage(msg)
			// 用户任意消息都代表活跃，非阻塞发送活跃信号重置超时
			select {
			case isLive <- true:
			default:
			}
		}
	}()

	// 超时定时器：2000秒（约33分钟）无任何消息则判定不活跃，强制踢出
	// 说明：原设计为长连接保活示例，可按需调小（如 300 秒）
	timer := time.NewTimer(2000 * time.Second)
	defer timer.Stop()

	// 主循环阻塞：等待关闭信号 / 活跃信号 / 超时
	for {
		select {
		case <-server.closeChan:
			// 服务器关闭：通知用户并下线
			user.SendMsg("服务器即将关闭，连接断开")
			user.Offline()
			return
		case <-done:
			// 读协程已退出（断线/异常），本 Handler 任务结束
			return
		case <-isLive:
			// 用户有消息：重置计时器
			if timer.Stop() {
				<-timer.C // 排空旧计时器，避免脏数据
			}
			timer.Reset(2000 * time.Second)
		case <-timer.C:
			// 超时：强制下线
			user.SendMsg("长时间不活跃，已被踢出房间")
			user.Offline()
			return
		}
	}
}

// Start 启动 TCP 监听服务（阻塞在 Accept 循环中）
// 注意：信号处理统一收敛在 main.go（SIGINT/SIGTERM → 完整优雅关闭流程），
// 本包不再自行监听信号与 os.Exit，避免与 main 的关闭顺序冲突（如 db.Flush/redis.Close 被跳过）
func (server *Server) Start() {
	// 监听端口
	listenAddr := fmt.Sprintf("%s:%d", server.Ip, server.Port)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("[tcp] 监听端口失败 %s: %v", listenAddr, err)
		return
	}
	defer listener.Close()
	log.Printf("[tcp] 聊天室服务启动成功，监听地址：%s", listenAddr)

	// 无限循环，持续等待新客户端接入
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.closeChan:
				return // 服务已关闭，退出 Accept 循环
			default:
				// listener 被外部关闭时（如 main 优雅关闭中）会短暂报错，
				// 小睡后再试，避免空转刷日志
				log.Printf("[tcp] Accept接收连接异常: %v", err)
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}
		// 每条新连接开启独立协程处理
		go server.Handler(conn)
	}
}

// Close 安全关闭服务：
// 1. 广播关闭信号 2. 全部用户强制下线 3. 等待所有 Handler 协程退出
func (server *Server) Close() {
	// 用 Once 防止重复调用时 close(closeChan) panic
	server.closeOnce.Do(func() {
		close(server.closeChan)
		// 全部在线用户强制下线（Offline 内部幂等，重复调用安全）
		online := server.GetAllOnlineUsers()
		for _, user := range online {
			user.Offline()
		}
		// 等待所有客户端 Handler 协程退出，确保资源全部释放
		server.wg.Wait()
		log.Println("[tcp] TCP服务已安全关闭，所有连接释放完成")
	})
}
