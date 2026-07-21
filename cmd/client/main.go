package main

import (
	"flag" //flag：解析程序启动时的命令行参数
	"fmt"
	"io"
	"net"
	"os"      //os：操作系统标准输入输
	"strconv" //strconv：类型转换，数字转字符串
	"strings"
)

type Client struct {
	ServerIP   string
	ServerPort int
	Name       string
	conn       net.Conn
	flag       int
}

func NewClient(serverIP string, serverPort int) *Client {
	//初始化客户端对象
	client := &Client{
		ServerIP:   serverIP,
		ServerPort: serverPort,
		flag:       10,
	}

	//链接server
	conn, err := net.Dial("tcp", serverIP+":"+strconv.Itoa(serverPort)) //net.Dial 主动发起TCP连接，拼接地址 ip:port
	if err != nil {
		fmt.Println("net.Dial 连接服务端失败: ", err)
		return nil
	}
	//保存TCP连接到客户端对象
	client.conn = conn
	return client
}

// DealResponse 处理server回应的消息， 直接显示到标准输出
func (client *Client) DealResponse() {
	//一旦client.conn有数据就直接copy到stdout标准输出上，永久阻塞监听，连接断开后函数才会结束
	_, err := io.Copy(os.Stdout, client.conn) //io.Copy(输出目标, 数据源)
	if err != nil && err != io.EOF {
		fmt.Println("服务器消息读取中断: ", err)
	}
	fmt.Println("服务器连接已断开，程序即将退出")
	// 服务端断连，强制退出程序
	os.Exit(1)
}

// 用户菜单选择
func (client *Client) menu() bool {
	var flags int
	fmt.Println("-----功能菜单-----")
	fmt.Println("1.公聊模式")
	fmt.Println("2.私聊模式")
	fmt.Println("3.更新用户名")
	fmt.Println("0.退出")

	//读取控制台输入数字
	_, err := fmt.Scanf("%d", &flags)
	if err != nil {
		// 输入非数字，清空缓冲区
		_, _ = fmt.Scanln()
		fmt.Println("输入错误")
		return false
	}
	if flags >= 0 && flags <= 3 {
		client.flag = flags
		// 清空换行缓冲区，避免后续Scanln读到空
		_, _ = fmt.Scanln()
		return true
	}
	fmt.Println("---请输入合法范围内的数字---")
	return false
}

// PublicChant 公聊模式
func (client *Client) PublicChant() {
	var chatMsg string
	for {
		fmt.Println("-----公聊模式，输入exit返回菜单-----")
		fmt.Print("输入消息：")
		_, _ = fmt.Scanln(&chatMsg)
		chatMsg = strings.TrimSpace(chatMsg)
		// 退出当前公聊模式
		if chatMsg == "exit" {
			break
		}
		// 空消息跳过发送
		if len(chatMsg) == 0 {
			fmt.Println("消息不能为空！")
			continue
		}
		// 协议：消息+\n 发给服务端
		sendMsg := chatMsg + "\n"
		_, err := client.conn.Write([]byte(sendMsg))
		if err != nil {
			fmt.Printf("发送消息失败，连接已断开: %v\n", err)
			return
		}
	}
}

// SelectUsers 发送who指令，请求服务端返回全部在线用户列表
func (client *Client) SelectUsers() {
	sendMsg := "who\n"
	_, err := client.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("请求用户列表失败:", err)
		return
	}
}

// PrivateChat 私聊模式
func (client *Client) PrivateChat() {
	var remoteName string
	var chatMsg string

	for remoteName != "exit" {
		// 刷新在线用户列表
		client.SelectUsers()
		fmt.Println("-----请选择聊天对象，exit退出-----")
		_, _ = fmt.Scanln(&remoteName)
		remoteName = strings.TrimSpace(remoteName)
		if remoteName == "exit" {
			break
		}
		if remoteName == "" {
			fmt.Println("用户名不能为空！")
			continue
		}
		// 持续给该用户发消息
		for {
			fmt.Print("输入私聊消息：")
			_, _ = fmt.Scanln(&chatMsg)
			chatMsg = strings.TrimSpace(chatMsg)
			if chatMsg == "exit" {
				break // 退出消息输入，重新选择私聊对象
			}
			if chatMsg == "" {
				fmt.Println("消息不能为空！")
				continue
			}
			// 私聊协议格式：to 用户名:消息\n
			sendMsg := fmt.Sprintf("to %s:%s\n", remoteName, chatMsg)
			_, err := client.conn.Write([]byte(sendMsg))
			if err != nil {
				fmt.Printf("私聊消息发送失败: %v\n", err)
				return
			}
		}
	}
}

// UpdateName 更新用户名模块
func (client *Client) UpdateName() bool {
	fmt.Println("-----请输入用户名-----")
	_, _ = fmt.Scanln(&client.Name)
	newName := strings.TrimSpace(client.Name)
	if newName == "" {
		fmt.Println("用户名不能为空！")
		return false
	}
	// 改名协议：rename:昵称
	sendMsg := "rename:" + client.Name + "\n"
	_, err := client.conn.Write([]byte(sendMsg))
	if err != nil {
		fmt.Println("更新用户名失败:", err)
		return false
	}
	return true
}

// Run 用户菜单使用
func (client *Client) Run() {
	for client.flag != 0 {
		for !client.menu() {
		}
		//处理不同模式的业务
		switch client.flag {
		case 1:
			client.PublicChant()
		case 2:
			client.PrivateChat()
		case 3:
			client.UpdateName()
		case 0:
			fmt.Println("正在退出客户端...")
			return
		}
	}
}

var serverIP string
var serverPort int

// init函数：程序main执行前自动运行，初始化命令行参数解析规则
func init() {
	flag.StringVar(&serverIP, "ip", "127.0.0.1", "设置服务器IP地址(默认地址为127.0.0.1)")
	flag.IntVar(&serverPort, "port", 8888, "设置服务器端口(默认端口为8888)")
}

func main() {
	//解析命令行输入的参数，赋值给serverIP、serverPort
	flag.Parse()

	client := NewClient(serverIP, serverPort)
	if client == nil {
		fmt.Println("-----链接服务器失败-----")
		return
	}
	fmt.Println("-----链接服务器成功-----")
	//单独开启一个goroutine去处理server的回执消息
	go client.DealResponse()
	//启动客户端业务
	client.Run()
	// 正常退出，关闭TCP连接释放资源
	_ = client.conn.Close()
	fmt.Println("客户端已安全退出")
}
