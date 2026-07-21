package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisClient 全局redis单例
var RedisClient *redis.Client
var ctx = context.Background()

// PubSub 订阅器类型别名，对外暴露
type PubSub = redis.PubSub

// InitRedis 初始化redis连接池
func InitRedis(addr, password string, db int) error {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     20, // 连接池大小
		MinIdleConns: 5,  // 最小空闲连接
		IdleTimeout:  5 * time.Minute,
	})
	// 连通性测试
	_, err := RedisClient.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("redis连接失败: %w", err)
	}
	fmt.Println("Redis连接初始化成功")
	return nil
}

// SetKV 写入string缓存，带过期时间
func SetKV(key string, val string, expire time.Duration) error {
	return RedisClient.Set(ctx, key, val, expire).Err()
}

// GetKV 获取string缓存
func GetKV(key string) (string, error) {
	return RedisClient.Get(ctx, key).Result()
}

// DelKV 删除key
func DelKV(key string) error {
	return RedisClient.Del(ctx, key).Err()
}

// LPush 左侧插入list（离线消息）
func LPush(key string, val string) error {
	return RedisClient.LPush(ctx, key, val).Err()
}

// LRange 获取list全部数据
func LRange(key string, start, stop int64) ([]string, error) {
	return RedisClient.LRange(ctx, key, start, stop).Result()
}

// DelList 清空list
func DelList(key string) error {
	return RedisClient.Del(ctx, key).Err()
}

// Publish 发布消息到频道
func Publish(channel string, msg string) error {
	return RedisClient.Publish(ctx, channel, msg).Err()
}

// Subscribe 订阅频道，返回订阅器
func Subscribe(channel string) *redis.PubSub {
	return RedisClient.Subscribe(ctx, channel)
}

// Close 关闭redis客户端
func Close() error {
	return RedisClient.Close()
}
