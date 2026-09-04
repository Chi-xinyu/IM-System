# IM-System — Go 语言 IM 聊天室

一个基于 **Go + Gin + WebSocket + TCP + Redis + SQLite** 的全栈聊天室项目。
支持 **Web 网页端** 与 **TCP 终端端** 双端接入、跨端互通、历史消息持久化、分布式广播、后台管理。

---

## ✨ 功能特性

| 能力 | 说明 |
|---|---|
| 双端接入 | 浏览器 WebSocket（`/api/ws`）+ 原生 TCP 客户端（端口 8888） |
| 跨端互通 | web 消息 ⇄ TCP 消息实时互通（基于 Redis Pub/Sub 统一分发） |
| 消息持久化 | SQLite 异步落库，历史消息接口 `/api/history` |
| 在线管理 | `/api/online` 全端在线列表（web + tcp 合并） |
| 管理员能力 | 全局公告、踢出指定用户（web/TCP 均可踢）、一键断开全部 |
| TCP 终端指令 | `who` 查看在线 / `rename:新昵称` 改名 / `to 昵称: 消息` 私聊（支持离线缓存补发） |
| 服务端心跳 | WebSocket Ping/Pong 保活，自动清理僵尸连接 |
| 优雅关闭 | SIGINT/SIGTERM → 停止接入 → 关闭连接 → 落库 flush → 释放 Redis |
| 分布式扩展 | 消息经 Redis 频道广播，多实例部署天然互通 |

---

## 📁 项目结构

```
IM-System/
├── cmd/webgate/main.go        # Web 网关入口（Gin HTTP + WS + Hub 管理 + 优雅关闭）
├── internal/
│   ├── chatcore/
│   │   ├── server.go          # TCP 服务 + Redis 消费分发中枢 + 消息统一出口
│   │   └── user.go            # TCP 用户：指令解析（who/rename/to/公聊）+ 离线消息
│   ├── db/db.go               # SQLite 持久化（WAL 模式 + 异步写入 + Flush）
│   └── redis/redis_client.go  # Redis 客户端（在线缓存/离线消息/PubSub）
├── web/
│   ├── login.html             # 登录页（昵称校验）
│   ├── chat.html              # 聊天室主界面
│   └── index.html             # 后台管理页
├── test/
│   ├── e2e/main.go            # 端到端功能测试（24 项断言）
│   ├── stress/main.go         # 并发压力测试（30 用户并发收发/断开）
│   └── redisstub/main.go      # miniredis 桩（无 Docker 环境本地验证用）
├── Dockerfile                 # 多阶段构建（静态二进制 + 时区 + db 目录）
├── docker-compose.yml         # Redis + im-server 编排（含健康检查/持久卷）
└── .dockerignore
```

---

## 🚀 快速启动

### 方式一：Docker Compose（推荐）

```bash
docker compose up -d --build
```

- 网页端：http://localhost:8080/web/login.html
- 后台管理：http://localhost:8080/web/index.html
- TCP 端口：8888

### 方式二：本地运行

```bash
# 1. 启动 Redis（本机 6379）
redis-server --appendonly yes

# 2. 启动服务（默认 8080 / 8888）
go run ./cmd/webgate/main.go

# 自定义端口
PORT=8081 TCP_PORT=8889 go run ./cmd/webgate/main.go
```

### TCP 终端客户端用法

```bash
# 用 nc 或自写客户端连接 8888，直接输入消息即可公聊
nc 127.0.0.1 8888
rename:bob          # 改名
who                 # 查看在线
to alice: 你好      # 私聊（对方离线则缓存，上线自动补发）
随便打个字          # 公聊广播
```

---

## 🧠 架构设计：消息统一分发

```
    web 用户 ──ws──┐                       ┌── TCP 用户(本地)
                   ▼                       ▼
    chatcore.BroadcastMsg(sender, msg)    每实例 consumeRedisPubSub
    ├─ 1. db.SaveMsg 异步落库        ──►   ├─ 分发本地 TCP 用户 (user.C)
    └─ 2. redis.Publish 频道          ◄──  └─ 回调 → Hub → 分发本地 web 用户
                     im:broadcast
```

- **单一消息出口**：所有公聊消息统一走 `BroadcastMsg`（入库 + 发布），
  从根上避免了原版「本地广播 + Redis 广播」双路导致的**重复消息/重复入库**。
- **回环分发**：发送者所在实例同样从 Redis 消费并回环推送（含发送者本人），
  前端按 `sender == 我的昵称` 渲染左右气泡，无需本地回显。
- **天然多实例**：任何消息所有实例都会收到并分发，水平扩展无需改代码。

---

## ⚠️ 已知边界

- TCP 私聊为终端指令，仅 TCP 用户间可用；web 端暂不做私聊 UI（后端已给出明确提示）。
- 消息落库为异步通道（上限 1000 条缓冲），瞬时超量会丢弃并打印告警；优雅关闭时 `db.Flush()` 兜底。
- Redis 在线缓存 TTL 30 分钟：进程崩溃等异常下线场景下，昵称最长 30 分钟后才可被复用。
