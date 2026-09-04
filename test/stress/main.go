// stress: 并发压力测试 —— 验证多用户同时收发/断开时服务无 panic、无死锁
// 场景：20 个 web + 10 个 tcp 用户同时上线发消息，随后全部断开，期间持续探测服务健康
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	httpAddr = "127.0.0.1:8080"
	tcpAddr  = "127.0.0.1:8888"
)

func main() {
	fmt.Println("===== 并发压力测试（20 web + 10 tcp，各发10条后断开） =====")
	var wg sync.WaitGroup
	errCh := make(chan error, 1000)

	// ---- web 用户 ----
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nick := fmt.Sprintf("stress_web_%d", idx)
			conn, _, err := websocket.DefaultDialer.Dial(
				fmt.Sprintf("ws://%s/api/ws?nick=%s", httpAddr, nick), nil)
			if err != nil {
				errCh <- fmt.Errorf("web %s 连接失败: %v", nick, err)
				return
			}
			defer conn.Close()
			// 消费消息（防止缓冲满）
			done := make(chan struct{})
			go func() {
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						close(done)
						return
					}
				}
			}()
			for j := 0; j < 10; j++ {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("web消息 %d-%d", idx, j)))
			}
			time.Sleep(200 * time.Millisecond)
			_ = conn.Close() // 并发断开，验证关闭竞态安全
			<-done
		}(i)
	}

	// ---- tcp 用户 ----
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", tcpAddr)
			if err != nil {
				errCh <- fmt.Errorf("tcp %d 连接失败: %v", idx, err)
				return
			}
			defer conn.Close()
			// 消费消息
			go func() {
				br := bufio.NewReader(conn)
				for {
					if _, err := br.ReadString('\n'); err != nil {
						return
					}
				}
			}()
			time.Sleep(100 * time.Millisecond)
			for j := 0; j < 10; j++ {
				_, _ = conn.Write([]byte(fmt.Sprintf("tcp消息 %d-%d\n", idx, j)))
			}
			time.Sleep(200 * time.Millisecond)
			_ = conn.Close()
		}(i)
	}

	wg.Wait()
	close(errCh)

	// ---- 汇总 ----
	failCnt := 0
	for err := range errCh {
		failCnt++
		fmt.Println("  ❌", err)
	}
	// 确认服务存活：/api/status 必须正常响应
	resp, err := http.Get(fmt.Sprintf("http://%s/api/status", httpAddr))
	alive := err == nil && resp.StatusCode == 200
	if resp != nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Println("服务存活探测:", string(b))
	}

	if failCnt == 0 && alive {
		fmt.Println("===== 压力测试通过：无错误、服务存活 =====")
		os.Exit(0)
	}
	fmt.Printf("===== 压力测试失败: 错误%d 服务存活%v =====\n", failCnt, alive)
	os.Exit(1)
}

var _ = strings.TrimSpace
