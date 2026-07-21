package chatcore

import (
	"IM-System/internal/db"
	"IM-System/internal/redis"
	"encoding/json"
	"fmt"
	"io"
	"net" //net：网络编程，TCP监听、连接相关API
	"os"
	"os/signal"
	"strings"
	"sync" //sync：同步工具包，RWMutex读写互斥锁，解决map并发读写冲突
	"syscall"
	"time"
)

const pubSubChannel = "im:broadcast"

type Server struct {
	Ip        string
	Port      int
	OnlineMap map[string]*User //在线用户的列表
	mapLock   sync.RWMutex     //读写互斥锁，保护OnlineMap并发增删查，防止多协程panic
	Message   chan string      //消息广播的channel
	closeChan chan struct{}    // 服务关闭信号通道
	wg        sync.WaitGroup   //等待所有客户端协程退出
	sub       *redis.PubSub    // redis订阅器
}

// PubMsg 分布式广播消息体
type PubMsg struct {
	Sender string `json:"sender"`
	Msg    string `json:"msg"`
}

// NewServer 创建一个server接口
func NewServer(ip string, port int) *Server {
	server := &Server{
		Ip:        ip,
		Port:      port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string, 200),
		closeChan: make(chan struct{}),
	}
	// 订阅redis全局广播频道
	server.sub = redis.Subscribe(pubSubChannel)
	// 启动redis消息消费协程
	go server.consumeRedisPubSub()
	return server
}

// consumeRedisPubSub 消费redis分布式广播，分发给本地所有TCP客户端
func (server *Server) consumeRedisPubSub() {
	ch := server.sub.Channel()
	for {
		select {
		case <-server.closeChan:
			_ = server.sub.Close()
			return
		case msg := <-ch:
			var data PubMsg
			_ = json.Unmarshal([]byte(msg.Payload), &data)
			fullMsg := fmt.Sprintf("[%s] %s", data.Sender, data.Msg)
			onlineCopy := server.GetAllOnlineUsers()
			for _, u := range onlineCopy {
				select {
				case u.C <- fullMsg:
				default:
				}
			}
		}
	}
}

// AddUser 添加在线用户
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

// GetAllOnlineUsers 获取全部在线用户
func (server *Server) GetAllOnlineUsers() map[string]*User {
	copyMap := make(map[string]*User)
	server.mapLock.RLock()
	for k, v := range server.OnlineMap {
		copyMap[k] = v
	}
	server.mapLock.RUnlock()
	return copyMap
}

// GetUser 根据用户名查询用户
func (server *Server) GetUser(name string) (*User, bool) {
	server.mapLock.RLock()
	user, ok := server.OnlineMap[name]
	server.mapLock.RUnlock()
	return user, ok
}

// ListenMessager 监听Message广播消息channel的goroutine，一旦有消息就发送给全部的在线用户
func (server *Server) ListenMessager() {
	for {
		select {
		case <-server.closeChan:
			return
		case msg := <-server.Message:
			//遍历副本发送，缩小锁持有时间
			onlineUser := server.GetAllOnlineUsers()
			for _, user := range onlineUser {
				select {
				case user.C <- msg:
				default:
					//用户通道满，直接丢弃防止阻塞广播协程
				}
			}
		}
	}
}

// BroadCast 广播消息的方法
func (server *Server) BroadCast(user *User, msg string) {
	//拼接消息格式 [客户端IP:端口]用户消息
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(user.Name)
	sb.WriteString("]")
	sb.WriteString(msg)
	fullMsg := sb.String()
	server.Message <- fullMsg
	// 发布至redis频道，多实例同步
	pubData, _ := json.Marshal(PubMsg{
		Sender: user.Name,
		Msg:    msg,
	})
	_ = redis.Publish(pubSubChannel, string(pubData))
	// 异步入库
	_ = db.SaveMsg(user.Name, msg)
}

// Handler 单个TCP连接处理协程：用户创建、上线、消息循环、超时保活、下线清理
func (server *Server) Handler(conn net.Conn) {
	server.wg.Add(1)
	defer server.wg.Done()

	//当前链接的业务
	user := NewUser(conn, server)
	server.AddUser(user)
	user.Online()

	//监听用户是否活跃的channel
	isLive := make(chan bool)

	//接受客户端发送的消息
	go func() {
		buf := make([]byte, 1024*40)
		for {
			select {
			case <-server.closeChan:
				return
			default:
			}
			n, err := conn.Read(buf)
			if n == 0 {
				user.Offline()
				return
			}
			// 读取异常，断开连接
			if err != nil && err != io.EOF {
				fmt.Printf("客户端 %s 读取异常: %v\n", conn.RemoteAddr(), err)
				user.Offline()
				return
			}
			//提取用户的消息(去除'\n')
			msg := strings.TrimSuffix(string(buf[:n]), "\n") //去掉字符串末尾指定后缀
			msg = strings.TrimSuffix(msg, "\r")
			//处理用户信息
			user.DoMessage(msg)
			//用户的任意消息,表示用户活跃
			select {
			case isLive <- true:
			default:
			}
		}
	}()

	timer := time.NewTimer(time.Second * 2000)
	defer timer.Stop()

	//当前handler阻塞
	for {
		select {
		case <-server.closeChan:
			user.SendMsg("服务器即将关闭，连接断开")
			user.Offline()
			return
		case <-isLive:
			//重置定时器
			if timer.Stop() {
				<-timer.C
			}
			timer.Reset(time.Second * 2000)
		case <-timer.C:
			//已超时
			//强制关闭当前user
			user.SendMsg("长时间不活跃，已被踢出房间")
			user.Offline()
			return
		}
	}
}

// Start 启动服务器
func (server *Server) Start() {
	//socker listen
	listenAddr := fmt.Sprintf("%s:%d", server.Ip, server.Port)
	listener, err := net.Listen("tcp", listenAddr) //fmt.Sprintf拼接地址字符串
	if err != nil {
		fmt.Printf("监听端口失败 %s: %v\n", listenAddr, err)
		return
	}
	defer func(listener net.Listener) {
		_ = listener.Close()
	}(listener)
	fmt.Printf("聊天室服务启动成功，监听地址：%s\n", listenAddr)

	//启动监听Message的goroutine
	go server.ListenMessager()

	// 监听系统信号：Ctrl+C(SIGINT)、进程终止(SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n收到关闭信号，开始关闭服务...")
		_ = listener.Close()
		server.Close()
		os.Exit(0)
	}()

	//无限循环，持续等待新客户端接入
	for {
		//阻塞等待客户端连接，有新连接才往下执行
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.closeChan:
				return
			default:
				fmt.Println("Accept接收连接异常:", err)
				continue //连接失败不终止服务，继续等待下一个客户端
			}
		}
		//每条新连接开启独立协程处理
		go server.Handler(conn)
	}
}

// Close 安全关闭服务
func (server *Server) Close() {
	close(server.closeChan)
	close(server.Message)
	online := server.GetAllOnlineUsers()
	//全部用户强制下线
	for _, user := range online {
		user.Offline()
	}
	//等待所有客户端Handler协程退出
	server.wg.Wait()
	fmt.Println("服务已安全关闭，所有连接释放完成")
}
