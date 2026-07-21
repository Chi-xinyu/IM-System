package db

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite" //匿名导入sqlite3驱动，只执行注册逻辑
)

// DB 全局单例，整个项目公用一个数据库连接
var DB *sql.DB

var msgChan chan struct {
	sender  string
	content string
}
var once sync.Once

// InitDB 初始化数据库，创建表并启动异步入库协程，程序启动时调用一次
func InitDB() error {
	dbPath := "./db/chat.db"
	dirPath := "./db"
	// 创建db目录，不存在则新建
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return fmt.Errorf("创建db文件夹失败：%w", err)
	}
	//打开sqlite文件，不存在则自动创建db/chat.db
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("打开数据库失败: %v", err)
	}
	DB = db

	// 异步消息通道，缓冲区1000
	once.Do(func() {
		msgChan = make(chan struct {
			sender  string
			content string
		}, 1000)
		// 启动独立协程异步写入数据库，不阻塞聊天主流程
		go func() {
			for item := range msgChan {
				now := time.Now().Format("2006-01-02 15:04:05")
				_, err := DB.Exec("INSERT INTO message (sender, content, create_time) VALUES(?, ?, ?)",
					item.sender, item.content, now)
				if err != nil {
					fmt.Printf("异步保存消息失败: %v\n", err)
				}
			}
		}()
	})

	//创建用户表：存储注册用户昵称（拓展账号登录用）
	createUserTable := `
	CREATE TABLE IF NOT EXISTS user (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE NOT NULL,
		create_time DATETIME
	);`
	_, err = db.Exec(createUserTable)
	if err != nil {
		return fmt.Errorf("创建用户表失败：%w", err)
	}

	//创建聊天消息表：存储所有公聊历史消息
	createMsgTable := `
	CREATE TABLE IF NOT EXISTS message (
	    id INTEGER PRIMARY KEY AUTOINCREMENT,
	    sender TEXT NOT NULL,
	    content TEXT NOT NULL,
	    create_time DATETIME NOT NULL
	);`
	_, err = db.Exec(createMsgTable)
	if err != nil {
		return fmt.Errorf("创建消息表失败：%w", err)
	}
	return nil
}

// SaveMsg 投递至异步通道
func SaveMsg(sender, content string) error {
	select {
	case msgChan <- struct {
		sender  string
		content string
	}{sender: sender, content: content}:
	default:
		fmt.Println("消息入库通道已满，丢弃消息")
	}
	return nil
}

// HistoryMsg 历史消息结构体，映射数据库message表字段
type HistoryMsg struct {
	ID         int
	Sender     string
	Content    string
	CreateTime string
}

// GetHistoryMsg 查询最近50条历史消息
func GetHistoryMsg() ([]HistoryMsg, error) {
	// 倒序查询最新50条
	sqlStr := `SELECT id, sender, content, create_time FROM message ORDER BY id DESC LIMIT 50;`
	rows, err := DB.Query(sqlStr)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var list []HistoryMsg
	for rows.Next() {
		var m HistoryMsg
		err = rows.Scan(&m.ID, &m.Sender, &m.Content, &m.CreateTime)
		if err != nil {
			return nil, err
		}
		list = append(list, m)
	}
	// 反转，让消息按时间正序展示（旧消息在前，新消息在后）
	for i, j := 0, len(list)-1; i < j; i, j = i+1, j-1 {
		list[i], list[j] = list[j], list[i]
	}
	return list, nil
}
