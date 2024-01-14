package windy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/visforest/windy/core"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// rClient is a client backed by redis, which implements core.Producer and core.Consumer
type rClient struct {
	ctx      context.Context
	rds      *redis.Client
	prefix   string
	queueKey string
}

func (c *rClient) Push(m *core.Msg) error {
	val, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.rds.LPush(c.ctx, c.queueKey, string(val)).Err()
}

func (c *rClient) Fetch() (*core.Msg, error) {
	vals, err := c.rds.BRPop(c.ctx, 0, c.queueKey).Result()
	if err != nil {
		return nil, err
	}
	var m core.Msg
	if err = json.Unmarshal([]byte(vals[1]), &m); err == nil {
		return &m, nil
	}
	return nil, err
}

type RProducer struct {
	producerCore *core.ProducerCore
	client       *rClient
}

// NewRProducer returns a producer and an error
func NewRProducer(cfg *RConf, opts ...core.ProducerOption) (*RProducer, error) {
	redisOpts, err := redis.ParseURL(cfg.Url)
	if err != nil {
		return nil, err
	}
	producerCore := &core.ProducerCore{
		Ctx:       context.Background(),
		Topic:     cfg.Topic,
		IdCreator: core.NewSnowflakeCreator(1),
	}
	for _, opt := range opts {
		opt(producerCore)
	}
	client := &rClient{
		ctx:      producerCore.Ctx,
		rds:      redis.NewClient(redisOpts),
		prefix:   cfg.KeyPrefix,
		queueKey: fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
	}
	return &RProducer{
		producerCore: producerCore,
		client:       client,
	}, nil
}

// MustNewRProducer returns a producer or panic if fails
func MustNewRProducer(cfg *RConf, opts ...core.ProducerOption) *RProducer {
	producer, err := NewRProducer(cfg, opts...)
	if err != nil {
		panic(err)
	}
	return producer
}

func (p *RProducer) Send(data any) (string, error) {
	return p.producerCore.Send(p.client, core.NewMsg(data))
}

type RConsumer struct {
	consumerCore *core.ConsumerCore
	client       *rClient
}

// NewRConsumer returns a consumer and error
func NewRConsumer(cfg *RConf, handler core.ConsumeFunc, opts ...core.ConsumerOption) (*RConsumer, error) {
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}

	redisOpts, err := redis.ParseURL(cfg.Url)
	if err != nil {
		return nil, err
	}
	consumerCore := &core.ConsumerCore{
		Ctx:                 context.Background(),
		Topic:               cfg.Topic,
		Processors:          cfg.Processors,
		ConsumeFunc:         handler,
		BatchProcessCnt:     cfg.BatchProcessConf.Batch,
		BatchProcessTimeout: time.Duration(cfg.BatchProcessConf.Timeout) * time.Second,
	}
	for _, opt := range opts {
		opt(consumerCore)
	}
	client := &rClient{
		ctx:      consumerCore.Ctx,
		rds:      redis.NewClient(redisOpts),
		prefix:   cfg.KeyPrefix,
		queueKey: fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
	}

	return &RConsumer{
		consumerCore: consumerCore,
		client:       client,
	}, nil
}

// MustNewRConsumer returns a consumer, if it fails, panic
func MustNewRConsumer(cfg *RConf, handler core.ConsumeFunc, opts ...core.ConsumerOption) *RConsumer {
	consumer, err := NewRConsumer(cfg, handler, opts...)
	if err != nil {
		panic(err)
	}
	return consumer
}

func (c *RConsumer) LoopConsume() {
	c.consumerCore.LoopConsume(c.client)
}
