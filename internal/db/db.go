package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	// 匿名导入 modernc.org/sqlite 纯 Go 实现，无需 CGO，方便 Docker 交叉编译
	_ "modernc.org/sqlite"
)

// DB 全局单例，整个项目公用一个数据库连接
var DB *sql.DB

// msgItem 异步入库消息的结构体（通过 channel 传递，避免阻塞聊天主流程）
type msgItem struct {
	sender  string
	content string
}

// msgChan 异步消息通道，缓冲区1000条，写满后丢弃并告警（聊天室对消息落库容忍一定丢失）
var msgChan chan msgItem

// pending 待落库消息计数：SaveMsg 投递时 +1，消费协程落库后 -1，
// 供 Flush() 在优雅关闭/测试时等待所有消息真正写入磁盘
var pending atomic.Int64

var once sync.Once

// InitDB 初始化数据库：创建 db 目录、打开 sqlite 文件、建表、启动异步写入协程
// 程序启动时调用一次
func InitDB() error {
	dbPath := "./db/chat.db"
	dirPath := "./db"
	// 创建db目录，不存在则新建（容器内挂载数据卷时目录由卷自动创建）
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("创建db文件夹失败：%w", err)
	}
	// 打开sqlite文件，不存在则自动创建 db/chat.db
	// 使用 URI 形式并开启关键 PRAGMA：
	//   - busy_timeout(5000)：读写并发时等待锁最多5秒，避免 busy 错误导致查询失败
	//   - journal_mode(WAL)：预写日志模式，读不阻塞写、写不阻塞读，聊天室读写并发友好
	dbConn, err := sql.Open("sqlite",
		"file:"+dbPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return fmt.Errorf("打开数据库失败: %v", err)
	}
	// 设置连接池参数：SQLite 单写多读，这里用少量连接即可
	dbConn.SetMaxOpenConns(4)
	dbConn.SetMaxIdleConns(2)
	DB = dbConn

	// 初始化消息异步写入通道与消费协程（sync.Once 保证只初始化一次）
	once.Do(func() {
		msgChan = make(chan msgItem, 1000)
		go func() {
			// 独立协程从通道取消息写库，不阻塞聊天主流程
			for item := range msgChan {
				now := time.Now().Format("2006-01-02 15:04:05")
				if _, err := DB.Exec(
					"INSERT INTO message (sender, content, create_time) VALUES(?, ?, ?)",
					item.sender, item.content, now,
				); err != nil {
					log.Printf("[db] 异步保存消息失败: %v", err)
				}
				// 落库完成，减少待处理计数（Flush 据此判断是否可安全退出）
				pending.Add(-1)
			}
		}()
	})

	// 创建用户表：预留账号注册能力（当前登录仅用昵称，此表留作扩展）
	createUserTable := `
	CREATE TABLE IF NOT EXISTS user (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE NOT NULL,
		create_time DATETIME
	);`
	if _, err := dbConn.Exec(createUserTable); err != nil {
		return fmt.Errorf("创建用户表失败：%w", err)
	}

	// 创建聊天消息表：存储所有公聊历史消息
	// create_time 存字符串便于前端直接展示，id 自增保证时序
	createMsgTable := `
	CREATE TABLE IF NOT EXISTS message (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    sender TEXT NOT NULL,
	    content TEXT NOT NULL,
	    create_time DATETIME NOT NULL
	);`
	if _, err := dbConn.Exec(createMsgTable); err != nil {
		return fmt.Errorf("创建消息表失败：%w", err)
	}
	return nil
}

// SaveMsg 投递消息到异步通道（非阻塞；通道满时丢弃并打印告警）
// 注意：本方法只负责"投递"，真正的 INSERT 由消费协程完成
func SaveMsg(sender, content string) error {
	// 防止 InitDB 未调用时 msgChan 为 nil 导致死锁（select 对 nil channel 永不就绪）
	if msgChan == nil {
		return fmt.Errorf("数据库尚未初始化")
	}
	select {
	case msgChan <- msgItem{sender: sender, content: content}:
		// 投递成功，登记一条待落库消息（Flush 等待依据）
		pending.Add(1)
	default:
		log.Printf("[db] 消息入库通道已满，丢弃消息: %s -> %s", sender, content)
	}
	return nil
}

// Flush 阻塞等待所有已投递消息落库完成（优雅关闭时调用，防止退出丢消息）
// 带超时保护：最多等待2秒，避免无限阻塞
func Flush() {
	if msgChan == nil {
		return
	}
	deadline := time.Now().Add(2 * time.Second)
	for pending.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// HistoryMsg 历史消息结构体，映射数据库message表字段
type HistoryMsg struct {
	ID         int    `json:"id"`
	Sender     string `json:"sender"`
	Content    string `json:"content"`
	CreateTime string `json:"createTime"`
}

// GetHistoryMsg 查询最近50条历史消息，按时间正序返回（旧在前，新在后）
// 说明：无参版本保留原项目调用兼容（旧代码都是 db.GetHistoryMsg() 无参调用）；
//
//	需要控制条数时请使用 GetHistoryMsgN(limit)
func GetHistoryMsg() ([]HistoryMsg, error) {
	return GetHistoryMsgN(50)
}

// GetHistoryMsgN 查询最近 limit 条历史消息，按时间正序返回（旧在前，新在后）
// limit <= 0 时使用默认值50；超过200按200截断，防止一次拉取过多
func GetHistoryMsgN(limit int) ([]HistoryMsg, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	// 先倒序查最新 limit 条，再在内存里反转成正序，避免子查询
	sqlStr := `SELECT id, sender, content, create_time FROM message ORDER BY id DESC LIMIT ?;`
	rows, err := DB.Query(sqlStr, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []HistoryMsg
	for rows.Next() {
		var m HistoryMsg
		if err = rows.Scan(&m.ID, &m.Sender, &m.Content, &m.CreateTime); err != nil {
			return nil, err
		}
		list = append(list, m)
	}
	// 反转：让消息按 id 正序展示（旧消息在前，新消息在后）
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}
	return list, nil
}

// GetMsgCount 返回历史消息总数（后台管理页展示用）
func GetMsgCount() (int, error) {
	var cnt int
	err := DB.QueryRow(`SELECT COUNT(*) FROM message;`).Scan(&cnt)
	return cnt, err
}
