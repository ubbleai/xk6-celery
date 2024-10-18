package celery

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// RedisClient is an interface for the redis client methods we use.
type RedisClient interface {
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}

type RedisBroker struct {
	redisClient RedisClient
}

type SentinelEnvConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func NewRedisBrokerBackend(client RedisClient) *RedisBroker {
	return &RedisBroker{
		redisClient: client,
	}
}

func NewRedisClient(opts *options) *redis.Client {

	if len(opts.SentinelAddrs) == 0 {
		redisOpts, err := redis.ParseURL(opts.Url)
		if err != nil {
			panic(err)
		}

		return redis.NewClient(redisOpts)
	} else {

		failOverOptions := &redis.FailoverOptions{
			MasterName:      opts.MasterName,
			SentinelAddrs:   opts.SentinelAddrs,
			DialTimeout:     opts.GetRetryInterval.Duration,
			ReadTimeout:     opts.GetRetryInterval.Duration,
			WriteTimeout:    opts.GetRetryInterval.Duration,
			MaxRetryBackoff: opts.GetRetryInterval.Duration,
		}

		return redis.NewFailoverClient(failOverOptions)
	}

}

func (rb *RedisBroker) Publish(ctx context.Context, message []byte, rawMessage string, queue string) error {
	err := rb.redisClient.LPush(ctx, queue, message).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rb *RedisBroker) Get(ctx context.Context, taskID string) *redis.StringCmd {
	val := rb.redisClient.Get(ctx, taskID)
	return val
}
