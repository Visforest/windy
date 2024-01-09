package rq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/visforest/windy/model"
	"github.com/visforest/windy/plugins"
)

type ConsumeHandler func(ctx context.Context, topic string, msg *model.Msg) error

type Consumer struct {
	ctx      context.Context
	client   *redis.Client
	topic    string
	prefix   string
	queueKey string
	listener plugins.ConsumeListener

	handler    ConsumeHandler
	processors int
}

type ConsumerOption func(consumer *Consumer)

func WithConsumerContext(ctx context.Context) ConsumerOption {
	return func(c *Consumer) {
		c.ctx = ctx
	}
}

func WithConsumerListener(listener plugins.ConsumeListener) ConsumerOption {
	return func(c *Consumer) {
		c.listener = listener
	}
}

// NewConsumer returns a consumer and error
func NewConsumer(cfg *Conf, handler ConsumeHandler, opts ...ConsumerOption) (*Consumer, error) {
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}

	redisOpts, err := redis.ParseURL(cfg.Url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(redisOpts)
	consumer := &Consumer{
		ctx:        context.Background(),
		client:     client,
		topic:      cfg.Topic,
		prefix:     cfg.KeyPrefix,
		queueKey:   fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
		processors: cfg.Processors,
		handler:    handler,
	}
	for _, opt := range opts {
		opt(consumer)
	}
	return consumer, nil
}

// MustNewConsumer returns a consumer, if it fails, panic
func MustNewConsumer(cfg *Conf, handler ConsumeHandler, opts ...ConsumerOption) *Consumer {
	consumer, err := NewConsumer(cfg, handler, opts...)
	if err != nil {
		panic(err)
	}
	return consumer
}

// Consume consumes once in single goroutine, note that it'll block until a msg is received
func (c *Consumer) Consume() error {
	var err error
	var result []string
	if c.listener == nil {
		// before consume
		result, err = c.client.BRPop(c.ctx, 0, c.queueKey).Result()
		if err != nil {
			return err
		}
		var msg model.Msg
		if err = json.Unmarshal([]byte(result[1]), &msg); err != nil {
			return err
		}
		// consume
		err = c.handler(c.ctx, c.topic, &msg)
	} else {
		// before consume
		result, err = c.client.BRPop(c.ctx, 0, c.queueKey).Result()
		if err != nil {
			c.listener.PrepareConsume(c.ctx, c.topic, nil, err)
			return err
		}
		var msg model.Msg
		if err = json.Unmarshal([]byte(result[1]), &msg); err != nil {
			c.listener.PrepareConsume(c.ctx, c.topic, nil, err)
			return err
		}
		c.listener.PrepareConsume(c.ctx, c.topic, &msg, nil)

		// consume
		err = c.handler(c.ctx, c.topic, &msg)

		if err == nil {
			// on consume succeeds
			c.listener.OnConsumeSucceed(c.ctx, c.topic, &msg)
		} else {
			// on consume fails
			c.listener.OnConsumeFail(c.ctx, c.topic, &msg, err)
		}
	}
	return err
}

// LoopConsume blocks and consumes msgs in loop with multi goroutine
func (c *Consumer) LoopConsume() {
	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	for i := 0; i < c.processors; i++ {
		go func() {
			for {
				_ = c.Consume()
			}
		}()
	}
	sigVal := <-s
	fmt.Printf("got signal:%v, quiting \n", sigVal)
}
