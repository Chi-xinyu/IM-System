package main

import (
	"IM-System/internal/chatcore"
	"IM-System/internal/db"
	"IM-System/internal/redis"
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// WebSocket升级配置
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// 本地开发放行全部跨域
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var wsClients = make(map[*websocket.Conn]bool) //存储所有网页WebSocket连接
var wsLock sync.Mutex                          //保护wsClients并发增删

// broadcastWS 给所有网页WebSocket客户端广播消息
func broadcastWS(msg string) {
	wsLock.Lock()
	defer wsLock.Unlock()
	for conn := range wsClients {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
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
			onlineKeys, _ := redis.RedisClient.Keys(context.Background(), chatcore.OnlineUserKeyPrefix+"*").Result()
			list := make([]gin.H, 0, len(onlineKeys))
			for _, k := range onlineKeys {
				val, _ := redis.GetKV(k)
				var info chatcore.UserInfo
				_ = json.Unmarshal([]byte(val), &info)
				list = append(list, gin.H{
					"name": info.Name,
					"addr": info.Addr,
				})
			}
			c.JSON(http.StatusOK, gin.H{
				"code": 0,
				"data": list,
			})
		})

		//提出指定成员
		api.POST("/kick", func(c *gin.Context) {
			type Req struct {
				Username string `json:"username"`
			}
			var req Req
			if err := c.ShouldBind(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"msg": "参数错误",
				})
				return
			}
			user, ok := tcpServer.GetUser(req.Username)
			if !ok {
				c.JSON(http.StatusBadRequest, gin.H{
					"msg": "用户不存在",
				})
				return
			}
			user.Offline()
			c.JSON(http.StatusOK, gin.H{
				"msg": "踢出成功",
			})
		})

		api.GET("/ws", func(c *gin.Context) {
			conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"msg": "websocket升级失败"})
				return
			}
			defer func(conn *websocket.Conn) {
				_ = conn.Close()
			}(conn)
			// 将新网页连接存入全局map
			wsLock.Lock()
			wsClients[conn] = true
			wsLock.Unlock()
			// 持续循环读取网页发送的消息
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					wsLock.Lock()
					delete(wsClients, conn)
					wsLock.Unlock()
					break
				}
				text := string(data)
				webMsg := "[网页用户] " + text
				broadcastWS(webMsg)
				// 推送到TCP服务，并同步Redis PubSub分布式广播
				tcpServer.BroadCast(&chatcore.User{Name: "网页用户"}, text)
			}
		})
	}

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
