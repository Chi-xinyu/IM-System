# 构建阶段
FROM golang:1.26-alpine AS builder

WORKDIR /app

ENV GOPROXY=https://goproxy.cn,direct

COPY go.mod go.sum ./
RUN go mod download

# 复制全部源码
COPY . .

# 重点：指定编译路径，输出到 /app/im-webgate
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/im-webgate ./cmd/webgate/main.go

# 运行阶段
FROM alpine:latest
WORKDIR /app

# 现在builder里存在 /app/im-webgate 和 /app/web
COPY --from=builder /app/im-webgate ./
COPY --from=builder /app/web ./web

EXPOSE 8080
CMD ["./im-webgate"]