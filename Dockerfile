# ========== 构建阶段 ==========
# 使用 Go 官方镜像，goproxy.cn 加速依赖下载（国内网络）
FROM golang:1.26-alpine AS builder

WORKDIR /app
ENV GOPROXY=https://goproxy.cn,direct

# 先只拷贝 go.mod/go.sum，利用 Docker 层缓存：依赖未变化时不重复下载
COPY go.mod go.sum ./
RUN go mod download

# 复制全部源码
COPY . .

# 编译 Web 网关（内置 TCP 服务），输出静态二进制：
# CGO_ENABLED=0 纯静态编译，运行镜像无需任何动态库，体积更小
# GOOS=linux 目标平台显式指定
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/im-webgate ./cmd/webgate/main.go

# ========== 运行阶段 ==========
FROM alpine:latest
WORKDIR /app

# 时区：容器默认UTC，设置Asia/Shanghai保证入库时间正确
RUN apk add --no-cache tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone \
    #预创建db目录(docker-compose数据卷挂载点) \
    && mkdir -p /app/db

# 只拷贝编译产物与前端静态文件
COPY --from=builder /app/im-webgate ./
COPY --from=builder /app/web ./web

EXPOSE 8080 8888
CMD ["./im-webgate"]