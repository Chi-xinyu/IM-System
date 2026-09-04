// e2e: IM 聊天室端到端功能测试
// 覆盖：WS 连接/上线/公聊/TCP互通/系统消息/重名拒绝/历史消息/在线列表/公告/踢出
// 用法：go run ./test/e2e  （服务需已启动，默认 http://127.0.0.1:8080, tcp 127.0.0.1:8888）
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	httpAddr = "127.0.0.1:8080"
	tcpAddr  = "127.0.0.1:8888"
)

var passed, failed int

func report(name string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  ✅ PASS  %s\n", detail)
	} else {
		failed++
		fmt.Printf("  ❌ FAIL  %s (%s)\n", name, detail)
	}
}

// wsClient 简易 WebSocket 客户端（带收件箱与超时等待）
type wsClient struct {
	conn *websocket.Conn
	in   chan string
}

func newWSClient(nick string) (*wsClient, error) {
	conn, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/api/ws?nick=%s", httpAddr, nick), nil)
	if err != nil {
		return nil, err
	}
	c := &wsClient{conn: conn, in: make(chan string, 256)}
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				close(c.in)
				return
			}
			c.in <- string(data)
		}
	}()
	return c, nil
}

func (c *wsClient) send(s string) error {
	return c.conn.WriteMessage(websocket.TextMessage, []byte(s))
}

// waitMsg 等待指定子串出现（最多 timeout），消费掉之前的消息
func (c *wsClient) waitMsg(sub string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-c.in:
			if !ok {
				return false
			}
			if strings.Contains(m, sub) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// tcpClient 简易 TCP 客户端
type tcpClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func newTCPClient() (*tcpClient, error) {
	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	return &tcpClient{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (t *tcpClient) send(s string) error {
	_, err := t.conn.Write([]byte(s + "\n"))
	return err
}

func (t *tcpClient) readLine(timeout time.Duration) (string, error) {
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		line, err := t.r.ReadString('\n')
		ch <- res{line, err}
	}()
	select {
	case r := <-ch:
		return strings.TrimSpace(r.line), r.err
	case <-time.After(timeout):
		return "", fmt.Errorf("读取超时")
	}
}

// waitLine 读取多行直到包含子串
func (t *tcpClient) waitLine(sub string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		line, err := t.readLine(timeout)
		if err != nil {
			return false
		}
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}

func main() {
	// 用 run() 包装：保证 defer 清理（关闭连接、删除Redis在线缓存）一定执行，
	// 避免 os.Exit 跳过 defer 导致 Redis 残留脏数据影响下一轮测试
	os.Exit(run())
}

func run() int {
	fmt.Println("===== IM 聊天室端到端测试 =====")

	// ---------- 1. WS 用户 alice 上线 ----------
	fmt.Println("\n[1] WS 用户 alice 上线")
	alice, err := newWSClient("alice")
	report("ws-connect", err == nil, "alice WebSocket 连接成功")
	if err != nil {
		os.Exit(1)
	}
	defer alice.conn.Close()
	report("ws-online-msg", alice.waitMsg("[系统] alice 进入聊天室", 3*time.Second),
		"alice 收到上线系统消息")

	// ---------- 2. TCP 用户连入并 rename 为 bob ----------
	fmt.Println("\n[2] TCP 用户连入并 rename 为 bob")
	bob, err := newTCPClient()
	report("tcp-connect", err == nil, "TCP 连接成功")
	if err != nil {
		os.Exit(1)
	}
	defer bob.conn.Close()
	// TCP 默认昵称=地址，先收上线回环消息（统一系统消息格式）
	report("tcp-online-msg", bob.waitLine("进入聊天室", 3*time.Second), "TCP 用户收到上线消息")
	_ = bob.send("rename:bob")
	report("tcp-rename", bob.waitLine("已更新用户名为: bob", 3*time.Second), "TCP 用户改名为 bob")
	// 改完名，web 端 alice 应收到改名广播
	report("web-sees-tcp", alice.waitMsg("改名为 bob", 3*time.Second), "web 端看到 TCP 用户改名为 bob")

	// ---------- 3. 公聊互通（web -> tcp / tcp -> web） ----------
	fmt.Println("\n[3] 公聊互通")
	_ = alice.send("hello from alice")
	report("web2tcp", bob.waitLine("[alice] hello from alice", 3*time.Second), "TCP 端收到 web 消息")
	report("web-self-echo", alice.waitMsg("[alice] hello from alice", 3*time.Second), "web 发送者收到自己的消息回环")

	_ = bob.send("hi everyone from bob")
	report("tcp2web", alice.waitMsg("[bob] hi everyone from bob", 3*time.Second), "web 端收到 TCP 消息（互通）")
	report("tcp-self-echo", bob.waitLine("[bob] hi everyone from bob", 3*time.Second), "TCP 发送者收到回环")

	// ---------- 4. who 指令（在线列表应含 alice 与 bob） ----------
	fmt.Println("\n[4] who 指令")
	_ = bob.send("who")
	// who 结果每用户一行（可能多行），循环读取直到超时再整体断言
	whoBuf := ""
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		l, err := bob.readLine(500 * time.Millisecond)
		if err != nil {
			break
		}
		whoBuf += l + "\n"
		if strings.Contains(whoBuf, "bob: 在线") && strings.Contains(whoBuf, "alice: 在线") {
			break
		}
	}
	report("tcp-who", strings.Contains(whoBuf, "bob: 在线") && strings.Contains(whoBuf, "alice: 在线"),
		fmt.Sprintf("who 输出同时包含 bob 与 alice（全端在线）: %s", strings.ReplaceAll(whoBuf, "\n", " | ")))

	// ---------- 5. 私聊（TCP 之间） ----------
	fmt.Println("\n[5] TCP 私聊")
	// 先让第二个 TCP 用户 charlie 上线
	charlie, err := newTCPClient()
	if err != nil {
		os.Exit(1)
	}
	defer charlie.conn.Close()
	_, _ = charlie.readLine(2 * time.Second) // 吃掉上线回环
	_ = charlie.send("rename:charlie")
	_, _ = charlie.readLine(2 * time.Second)
	_ = bob.send("to charlie: 这是私聊")
	report("tcp-private", charlie.waitLine("[私聊]bob: 这是私聊", 3*time.Second), "charlie 收到 bob 私聊")
	// 私聊离线消息：发给不存在的用户 zzz
	_ = bob.send("to zzz: 离线消息测试")
	report("tcp-offline", bob.waitLine("对方当前离线", 3*time.Second), "私聊离线消息缓存提示")

	// ---------- 6. 重名拒绝 ----------
	fmt.Println("\n[6] 重名拒绝")
	dupConn, _, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/api/ws?nick=%s", httpAddr, "alice"), nil)
	if err != nil {
		report("dup-nick", true, "重复昵称连接被拒绝（握手失败）")
	} else {
		// 握手成功但服务端发送占用提示后关闭连接
		_, data, _ := dupConn.ReadMessage()
		_ = dupConn.Close()
		report("dup-nick", strings.Contains(string(data), "昵称已被占用"),
			"重复昵称收到占用提示: "+string(data))
	}

	// ---------- 7. HTTP API ----------
	fmt.Println("\n[7] HTTP API")
	// 消息是异步入库的，稍等片刻确保写库完成再查历史
	time.Sleep(500 * time.Millisecond)
	// /api/online
	online := httpGet("/api/online")
	var onlineRes struct {
		Code int `json:"code"`
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	_ = json.Unmarshal([]byte(online), &onlineRes)
	names := ""
	for _, u := range onlineRes.Data {
		names += u.Name + ","
	}
	report("api-online", onlineRes.Code == 0 && strings.Contains(names, "alice") && strings.Contains(names, "bob"),
		fmt.Sprintf("在线列表包含 alice/bob（实际: %s）", names))

	// /api/history 应包含 alice 的公聊消息
	hist := httpGet("/api/history?limit=50")
	var histRes struct {
		Code int `json:"code"`
		Data []struct {
			Sender  string `json:"sender"`
			Content string `json:"content"`
		} `json:"data"`
	}
	_ = json.Unmarshal([]byte(hist), &histRes)
	hasMsg := false
	for _, m := range histRes.Data {
		if m.Sender == "alice" && m.Content == "hello from alice" {
			hasMsg = true
		}
	}
	report("api-history", histRes.Code == 0 && hasMsg, "历史消息包含 alice 的 hello from alice")
	report("api-history-sys", func() bool {
		for _, m := range histRes.Data {
			if m.Sender == "系统" {
				return true
			}
		}
		return false
	}(), "历史消息包含系统消息（入库正常）")

	// /api/status
	status := httpGet("/api/status")
	report("api-status", strings.Contains(status, "onlineNum") && strings.Contains(status, "totalMsg"),
		"状态接口返回在线数与消息总数")

	// ---------- 8. 公告广播 ----------
	fmt.Println("\n[8] 系统公告")
	httpPost("/api/broadcast", `{"msg":"全体注意，系统维护"}`)
	report("broadcast-web", alice.waitMsg("[系统公告] 全体注意，系统维护", 3*time.Second), "web 端收到公告")
	report("broadcast-tcp", bob.waitLine("[系统公告] 全体注意，系统维护", 3*time.Second), "TCP 端收到公告")

	// ---------- 9. 踢出 ----------
	fmt.Println("\n[9] 管理员踢出")
	httpPost("/api/kick", `{"Username":"charlie"}`)
	report("kick-tcp", charlie.waitLine("您已被管理员移出聊天室", 3*time.Second), "charlie 收到被踢提示")
	_, err = charlie.readLine(2 * time.Second)
	report("kick-close", err != nil, "charlie 连接已被服务端关闭")
	// 被踢下线应广播给其他用户
	report("kick-broadcast", alice.waitMsg("[系统] charlie 离开聊天室", 3*time.Second), "web 端收到 charlie 下线消息")

	// ---------- 10. 昵称非法 ----------
	fmt.Println("\n[10] 非法昵称")
	_, resp2, err := websocket.DefaultDialer.Dial(
		fmt.Sprintf("ws://%s/api/ws?nick=%s", httpAddr, "bad<script>"), nil)
	if err != nil {
		report("bad-nick", true, "非法昵称连接被拒绝")
	} else {
		if resp2 != nil {
			_ = resp2.Body.Close()
		}
		report("bad-nick", false, "非法昵称竟握手成功")
	}

	// ---------- 汇总 ----------
	fmt.Printf("\n===== 测试汇总: 通过 %d, 失败 %d =====\n", passed, failed)
	if failed > 0 {
		return 1
	}
	fmt.Println("全部通过 ✅")
	return 0
}

func httpGet(path string) string {
	resp, err := http.Get("http://" + httpAddr + path)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func httpPost(path, body string) string {
	resp, err := http.Post("http://"+httpAddr+path, "application/json", strings.NewReader(body))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
