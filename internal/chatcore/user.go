package chatcore

import (
	"IM-System/internal/redis"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	OnlineUserKeyPrefix = "im:online:"
	offlineMsgPrefix    = "im:offline:"
	userTTL             = 30 * time.Minute
)

type User struct {
	Name     string
	Addr     string
	C        chan string
	conn     net.Conn //TCP长连接对象
	server   *Server
	exitChan chan struct{} //用户退出信号通道，用于安全关闭所有子协程
	isClosed bool
	closeMu  sync.Mutex // 保护isClosed并发读写
}

// UserInfo 存入Redis的用户结构体
type UserInfo struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// NewUser 创建一个用户的API
func NewUser(conn net.Conn, server *Server) *User {
	//RemoteAddr()获取客户端对端地址，转字符串得到IP+端口
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Name:     userAddr,
		Addr:     userAddr,
		C:        make(chan string, 50),
		conn:     conn,
		server:   server,
		exitChan: make(chan struct{}),
	}
	//启动监听当前user channel消息的go程
	go user.Listen()
	return user
}

// Online 用户的上线业务
func (user *User) Online() {
	//用户上线，将用户加入到onlineMap中
	user.server.mapLock.Lock() //读写锁
	user.server.OnlineMap[user.Name] = user
	user.server.mapLock.Unlock()

	// 写入Redis在线用户缓存
	info := UserInfo{Name: user.Name, Addr: user.Addr}
	data, _ := json.Marshal(info)
	_ = redis.SetKV(OnlineUserKeyPrefix+user.Name, string(data), userTTL)

	// 补发离线私聊消息
	msgList, err := redis.LRange(offlineMsgPrefix+user.Name, 0, -1)
	if err == nil && len(msgList) > 0 {
		user.SendMsg("=====您有离线私聊消息=====")
		for _, m := range msgList {
			user.SendMsg(m)
		}
		// 补发完成清空离线消息
		_ = redis.DelList(offlineMsgPrefix + user.Name)
	}

	//广播当前用户上线消息
	user.server.BroadCast(user, "已上线")
}

// Offline 用户的下线业务
func (user *User) Offline() {
	user.closeMu.Lock()
	defer user.closeMu.Unlock()
	// 如果已经关闭，直接返回，不再执行关闭通道逻辑
	if user.isClosed {
		return
	}
	// 标记为已关闭，后续调用Offline直接跳过
	user.isClosed = true

	//广播当前用户下线消息
	user.server.BroadCast(user, "下线")

	//用户下线，将用户从onlineMap中删除
	user.server.mapLock.Lock()
	delete(user.server.OnlineMap, user.Name)
	user.server.mapLock.Unlock()

	// 删除Redis在线缓存
	_ = redis.DelKV(OnlineUserKeyPrefix + user.Name)
	//发送退出信号，关闭Listen协程
	close(user.exitChan)
	//关闭消息通道，释放发送协程资源
	close(user.C)
	//关闭TCP连接
	_ = user.conn.Close()
}

// SendMsg 发送消息
func (user *User) SendMsg(msg string) {
	//用户是否存在
	if user == nil || user.conn == nil || user.isClosed {
		return
	}
	data := []byte(msg + "\r\n") //追加换行符
	_, err := user.conn.Write(data)
	//写入失败，下线用户
	if err != nil {
		user.Offline()
	}
}

// DoMessage 用户处理消息的业务
func (user *User) DoMessage(msg string) {
	msg = strings.TrimSpace(msg) //移除字符串首尾所有空白字符，中间空白不动
	if len(msg) == 0 {
		return
	}
	switch {
	case msg == "who":
		//查询在线用户功能
		var sb strings.Builder //减少字符串拼接GC

		// 优先读取Redis全部在线用户
		onlineKeys, _ := redis.RedisClient.Keys(context.Background(), OnlineUserKeyPrefix+"*").Result()
		for _, k := range onlineKeys {
			val, _ := redis.GetKV(k)
			var info UserInfo
			_ = json.Unmarshal([]byte(val), &info)
			sb.WriteString("[")
			sb.WriteString(info.Addr)
			sb.WriteString("]")
			sb.WriteString(info.Name)
			sb.WriteString(": 在线\n")
		}
		user.SendMsg(sb.String())

	case strings.HasPrefix(msg, "rename:"):
		//更新用户名功能
		parts := strings.Split(msg, ":")
		//容错：分割后长度不足直接返回，防止索引越界panic
		if len(parts) < 2 {
			user.SendMsg("格式有误，请使用 rename:昵称")
			return
		}
		newName := strings.TrimSpace(parts[1])
		if newName == "" {
			user.SendMsg("用户名不能为空")
			return
		}
		// 校验Redis是否存在该昵称
		_, err := redis.GetKV(OnlineUserKeyPrefix + newName)
		if err == nil {
			user.SendMsg("该用户名已被其他用户占用，请更换")
			return
		}
		// 删除旧redis key
		_ = redis.DelKV(OnlineUserKeyPrefix + user.Name)
		//修改map键
		user.server.mapLock.Lock()
		delete(user.server.OnlineMap, user.Name)
		user.Name = newName
		user.server.OnlineMap[newName] = user
		user.server.mapLock.Unlock()

		// 写入新redis key
		info := UserInfo{Name: user.Name, Addr: user.Addr}
		data, _ := json.Marshal(info)
		_ = redis.SetKV(OnlineUserKeyPrefix+user.Name, string(data), userTTL)
		user.SendMsg("已更新用户名为: " + user.Name + "\n")

	case strings.HasPrefix(msg, "to"):
		//私聊指令 to 用户名:消息内容
		//拆分第一层 "to" 和后半段
		firstSplit := strings.SplitN(msg, " ", 2) //用分隔符 sep 切割字符串 s，最多切 n 份
		if len(firstSplit) < 2 {
			user.SendMsg("消息格式不正确，请使用: to 昵称: 消息\n")
			return
		}
		targetPart := firstSplit[1]
		//拆分用户名和聊天内容
		targetInfo := strings.SplitN(targetPart, ":", 2)
		if len(targetInfo) < 2 {
			user.SendMsg("消息格式不正确，请使用: to 昵称: 消息\n")
			return
		}
		//检查消息内容
		remoteName := strings.TrimSpace(targetInfo[0])
		content := strings.TrimSpace(targetInfo[1])
		if remoteName == "" {
			user.SendMsg("对象用户名不能为空\n")
			return
		}
		if content == "" {
			user.SendMsg("消息内容不能为空\n")
			return
		}

		// 查询Redis判断用户是否在线
		_, err := redis.GetKV(OnlineUserKeyPrefix + remoteName)
		if err != nil {
			// 用户离线，存入Redis离线消息
			privateMsg := "[离线私聊]" + user.Name + "：" + content
			_ = redis.LPush(offlineMsgPrefix+remoteName, privateMsg)
			user.SendMsg("对方当前离线，消息已存入离线缓存，上线自动推送")
			return
		}
		// 查询目标用户
		user.server.mapLock.Lock()
		remoteUser, ok := user.server.OnlineMap[remoteName]
		user.server.mapLock.Unlock()
		if !ok {
			user.SendMsg("私聊对象不存在或已下线")
			return
		}
		//发送私聊消息给对方
		privateMsg := "[私聊]" + user.Name + ": " + content
		remoteUser.SendMsg(privateMsg)

	default:
		//普通公聊消息，全局广播
		user.server.BroadCast(user, msg)
	}
}

// Listen 监听当前user channel，一旦有消息，就直接发送给对方对端客户端
func (user *User) Listen() {
	//无限循环，持续等待消息输入
	for {
		select {
		case <-user.exitChan:
			//收到退出信号，直接结束协程
			return
		case msg := <-user.C:
			//发送广播消息，底层SendMsg会捕获错误自动下线
			user.SendMsg(msg)
		}
	}
}
