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

// Redis key 前缀与有效期常量
const (
	OnlineUserKeyPrefix = "im:online:"     // 在线用户缓存 key 前缀（web/tcp 共用）
	offlineMsgPrefix    = "im:offline:"    // 离线私聊消息 list key 前缀
	userTTL             = 30 * time.Minute // 在线缓存有效期，防止异常下线遗留脏数据
)

// User TCP 客户端用户对象
type User struct {
	Name     string      // 用户名（默认=对端地址，可通过 rename: 修改）
	Addr     string      // 客户端地址 IP:端口
	C        chan string // 发给该用户的消息通道（Listen 协程取出后写入TCP连接）
	conn     net.Conn    // TCP 长连接对象
	server   *Server
	exitChan chan struct{} // 用户退出信号通道，用于安全关闭 Listen 协程
	isClosed bool
	closeMu  sync.Mutex // 保护 isClosed 并发读写，保证 Offline 幂等
}

// UserInfo 存入 Redis 在线缓存的用户结构体
type UserInfo struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// NewUser 创建用户对象：默认昵称=对端地址，并启动消息监听协程
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String() // RemoteAddr() 获取对端 IP:端口
	user := &User{
		Name:     userAddr,
		Addr:     userAddr,
		C:        make(chan string, 50), // 缓冲50条，防阻塞广播
		conn:     conn,
		server:   server,
		exitChan: make(chan struct{}),
	}
	// 启动监听 user.C 通道的协程：有消息就写入 TCP 连接
	go user.Listen()
	return user
}

// Online 用户上线业务：写Redis在线缓存 → 补发离线私聊 → 广播上线消息
func (user *User) Online() {
	// 写入 Redis 在线用户缓存（who/私聊/重名检测都依赖它）
	SetUserOnline(user.Name, user.Addr)

	// 补发离线私聊消息（之前不在线时别人 to 给自己的消息）
	msgList, err := redis.LRange(offlineMsgPrefix+user.Name, 0, -1)
	if err == nil && len(msgList) > 0 {
		user.SendMsg("=====您有离线私聊消息=====")
		for _, m := range msgList {
			user.SendMsg(m)
		}
		// 补发完成清空离线消息
		_ = redis.DelList(offlineMsgPrefix + user.Name)
	}

	// 广播上线消息（统一系统消息格式，web/tcp 全端可见）
	BroadcastMsg("系统", user.Name+" 进入聊天室")
}

// Offline 用户下线业务：从在线表移除 → 广播下线 → 删Redis缓存 → 关闭协程与连接
// 通过 isClosed + closeMu 保证幂等（多路径可能重复触发：读协程、超时、服务关闭、被踢）
//
// 【并发安全关键】user.C 通道只关闭 exitChan，不关闭 C：
//   - Listen 协程靠 exitChan 退出，无需关闭 C；
//   - 若关闭 C，而 consumeRedisPubSub 协程可能正持有 OnlineMap 副本向该用户发送，
//     就会触发 "send on closed channel" panic 导致整个进程崩溃（线上已复现）。
//   - 不关闭 C，残余消息只会写入缓冲或被 default 分支丢弃，由 GC 回收，绝对安全。
func (user *User) Offline() {
	user.closeMu.Lock()
	defer user.closeMu.Unlock()
	if user.isClosed {
		return // 已关闭过，直接跳过
	}
	user.isClosed = true

	// 1) 先从在线表移除（之后 GetAllOnlineUsers 不再返回本用户，新消息不再投递）
	user.server.mapLock.Lock()
	delete(user.server.OnlineMap, user.Name)
	user.server.mapLock.Unlock()

	// 2) 广播下线消息（统一系统消息格式，全端可见）
	BroadcastMsg("系统", user.Name+" 离开聊天室")
	// 3) 删除 Redis 在线缓存
	DelUserOnline(user.Name)
	// 4) 发送退出信号，关闭 Listen 协程（注意：不关闭 user.C，见上方说明）
	close(user.exitChan)
	// 5) 关闭 TCP 连接
	_ = user.conn.Close()
}

// SendMsg 向用户 TCP 连接写入一条消息（追加换行符）
// 写入失败说明连接已断开，触发 Offline 清理
func (user *User) SendMsg(msg string) {
	if user == nil || user.conn == nil || user.isClosed {
		return
	}
	data := []byte(msg + "\r\n")
	if _, err := user.conn.Write(data); err != nil {
		user.Offline()
	}
}

// DoMessage 处理用户发来的指令与消息（聊天协议解析）
// 支持的指令：
//
//	who                    → 查看在线用户
//	rename:新昵称          → 修改昵称
//	to 昵称: 消息          → 私聊（对方离线则缓存）
//	其他任意内容           → 公聊广播
func (user *User) DoMessage(msg string) {
	msg = strings.TrimSpace(msg) // 移除首尾空白，中间空白不动
	if len(msg) == 0 {
		return
	}
	switch {
	case msg == "who":
		// 查询在线用户：优先读 Redis（web + tcp 全端在线列表）
		var sb strings.Builder // strings.Builder 减少字符串拼接GC
		onlineKeys, _ := redis.RedisClient.Keys(context.Background(), OnlineUserKeyPrefix+"*").Result()
		for _, k := range onlineKeys {
			val, _ := redis.GetKV(k)
			var info UserInfo
			if err := json.Unmarshal([]byte(val), &info); err != nil {
				continue // 脏数据跳过
			}
			sb.WriteString("[")
			sb.WriteString(info.Addr)
			sb.WriteString("]")
			sb.WriteString(info.Name)
			sb.WriteString(": 在线\n")
		}
		user.SendMsg(sb.String())

	case strings.HasPrefix(msg, "rename:"):
		// 修改昵称指令
		parts := strings.SplitN(msg, ":", 2)
		if len(parts) < 2 {
			user.SendMsg("格式有误，请使用 rename:昵称")
			return
		}
		newName := strings.TrimSpace(parts[1])
		if newName == "" {
			user.SendMsg("用户名不能为空")
			return
		}
		// 校验新昵称是否已被占用（Redis 中已存在 = web/tcp 任一端在线）
		if IsUserOnline(newName) {
			user.SendMsg("该用户名已被其他用户占用，请更换")
			return
		}
		// 删除旧 Redis key，更新 map 键名，写入新 Redis key
		oldName := user.Name
		DelUserOnline(oldName)
		user.server.mapLock.Lock()
		delete(user.server.OnlineMap, oldName)
		user.Name = newName
		user.server.OnlineMap[newName] = user
		user.server.mapLock.Unlock()
		SetUserOnline(user.Name, user.Addr)
		user.SendMsg("已更新用户名为: " + user.Name + "\n")
		// 通知全员改名（统一分发，web/tcp 全端可见）
		BroadcastMsg("系统", oldName+" 改名为 "+newName)

	case msg == "to" || strings.HasPrefix(msg, "to "):
		// 私聊指令：to 昵称: 消息内容
		// 注意：必须精确匹配 "to" 或以 "to " 开头，
		// 原来用 strings.HasPrefix(msg, "to") 会把 "today你好" 等误判为私聊指令
		firstSplit := strings.SplitN(msg, " ", 2) // 先切出指令与后半段
		if len(firstSplit) < 2 {
			user.SendMsg("消息格式不正确，请使用: to 昵称: 消息")
			return
		}
		targetInfo := strings.SplitN(firstSplit[1], ":", 2) // 再切出昵称与内容
		if len(targetInfo) < 2 {
			user.SendMsg("消息格式不正确，请使用: to 昵称: 消息")
			return
		}
		remoteName := strings.TrimSpace(targetInfo[0])
		content := strings.TrimSpace(targetInfo[1])
		if remoteName == "" {
			user.SendMsg("对象用户名不能为空")
			return
		}
		if content == "" {
			user.SendMsg("消息内容不能为空")
			return
		}

		// 判断目标是否在线（Redis 缓存，全端统一）
		if !IsUserOnline(remoteName) {
			// 离线：消息存入 Redis list，对方上线时自动补发
			privateMsg := "[离线私聊]" + user.Name + "：" + content
			_ = redis.LPush(offlineMsgPrefix+remoteName, privateMsg)
			user.SendMsg("对方当前离线，消息已存入离线缓存，上线自动推送")
			return
		}
		// 在线：从本地 OnlineMap 找目标 TCP 用户
		remoteUser, ok := user.server.GetUser(remoteName)
		if !ok {
			// Redis 在线但本地 map 无 = 对方是网页端用户（web 端暂不支持私聊）
			user.SendMsg("对方为网页端用户，暂不支持私聊，请使用公聊")
			return
		}
		// 点对点发送私聊消息
		privateMsg := "[私聊]" + user.Name + ": " + content
		remoteUser.SendMsg(privateMsg)

	default:
		// 普通公聊消息：统一分发（入库 + Redis），全端广播
		user.server.BroadCast(user, msg)
	}
}

// Listen 监听用户消息通道：一旦有消息就写入 TCP 连接（常驻协程）
// Offline 时 close(exitChan) 与 close(C)，两种退出路径都安全
func (user *User) Listen() {
	for {
		select {
		case <-user.exitChan:
			return // 收到退出信号，直接结束协程
		case msg, ok := <-user.C:
			// ok=false 说明通道已被 Offline 关闭，结束协程
			// （原代码只写 <-user.C，通道关闭后会读到零值空串反复发送）
			if !ok {
				return
			}
			user.SendMsg(msg) // 底层 SendMsg 捕获错误自动下线
		}
	}
}
