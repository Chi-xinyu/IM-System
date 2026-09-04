package redis

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisClient 全局redis单例，整个进程共用连接池
var RedisClient *redis.Client

var ctx = context.Background()

// PubSub 订阅器类型别名，对外暴露（供 chatcore 订阅频道使用）
type PubSub = redis.PubSub

// InitRedis 初始化redis连接池，支持通过环境变量覆盖默认连接参数
// 环境变量：REDIS_ADDR(地址) REDIS_PWD(密码) REDIS_DB(库编号)
// docker-compose 中会注入 REDIS_ADDR=redis:6379，本地开发默认 127.0.0.1:6379
func InitRedis() error {
	addr := os.Getenv("REDIS_ADDR")
	pwd := os.Getenv("REDIS_PWD")
	dbIdx, _ := strconv.Atoi(os.Getenv("REDIS_DB")) // 解析失败默认为0，即DB 0
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	// 创建连接池：PoolSize 最大连接数、MinIdleConns 最小空闲连接、
	// IdleTimeout 空闲连接超时回收时间
	RedisClient = redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     pwd,
		DB:           dbIdx,
		PoolSize:     20,
		MinIdleConns: 5,
		IdleTimeout:  5 * time.Minute,
	})

	// Ping 验证连通性，连不上直接返回错误（调用方决定是panic还是降级）
	if err := RedisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis连接失败: %w", err)
	}
	log.Printf("[redis] 连接初始化成功, addr=%s db=%d", addr, dbIdx)
	return nil
}

// SetKV 写入string类型缓存，带过期时间（用于在线用户缓存 im:online:*）
func SetKV(key string, val string, expire time.Duration) error {
	return RedisClient.Set(ctx, key, val, expire).Err()
}

// GetKV 获取string类型缓存
func GetKV(key string) (string, error) {
	return RedisClient.Get(ctx, key).Result()
}

// DelKV 删除指定key
func DelKV(key string) error {
	return RedisClient.Del(ctx, key).Err()
}

// LPush 左侧插入list（用于暂存离线私聊消息）
func LPush(key string, val string) error {
	return RedisClient.LPush(ctx, key, val).Err()
}

// LRange 获取list指定区间数据（0,-1 表示取全部）
func LRange(key string, start, stop int64) ([]string, error) {
	return RedisClient.LRange(ctx, key, start, stop).Result()
}

// DelList 删除整个list（离线消息补发完成后清空）
func DelList(key string) error {
	return RedisClient.Del(ctx, key).Err()
}

// Publish 发布消息到指定频道（分布式广播核心，所有实例订阅同一频道）
func Publish(channel string, msg string) error {
	return RedisClient.Publish(ctx, channel, msg).Err()
}

// Subscribe 订阅频道，返回订阅器（消费协程从 Channel() 读取消息）
func Subscribe(channel string) *redis.PubSub {
	return RedisClient.Subscribe(ctx, channel)
}

// Close 关闭redis客户端（进程优雅退出时调用，释放连接池）
func Close() error {
	if RedisClient == nil {
		return nil
	}
	return RedisClient.Close()
}
