package windy

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Visforest/goset/v2"
	"github.com/visforest/windy/core"

	"github.com/redis/go-redis/v9"
)

var (
	scriptFetchDelayMsgs = redis.NewScript(`return redis.call('zrange', KEYS[1], 0, ARGV[1], 'with score')`)
)

// rClient is a client backed by redis, which implements core.Producer and core.Consumer
type rClient struct {
	ctx           context.Context
	rds           *redis.Client
	prefix        string
	queueKey      string
	delayQueueKey string
}

func (c *rClient) Push(m *core.Msg) error {
	val, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if m.DelayAt != nil {
		// delay msg
		err = c.rds.ZAdd(c.ctx, c.delayQueueKey, redis.Z{
			Score:  float64(m.DelayAt.Unix()),
			Member: string(val),
		}).Err()
	} else {
		// normal msg
		err = c.rds.LPush(c.ctx, c.queueKey, string(val)).Err()
	}
	return err
}

func (c *rClient) Fetch() (*core.Msg, error) {
	vals, err := c.rds.BRPop(c.ctx, 0, c.queueKey).Result()
	if err != nil {
		return nil, err
	}
	return core.DecodeMsgFromStr(vals[1])
}

func (c *rClient) FetchDelayMsgs() ([]*core.Msg, error) {
	r, err := scriptFetchDelayMsgs.Eval(c.ctx, c.rds, []string{c.delayQueueKey}, []int64{time.Now().Unix()}).Result()
	if err != nil {
		return nil, err
	}
	resultMsgStrs := r.([]string)
	msgs := make([]*core.Msg, len(resultMsgStrs))
	for i, msgStr := range resultMsgStrs {
		m, err := core.DecodeMsgFromStr(msgStr)
		if err != nil {
			return nil, err
		}
		msgs[i] = m
	}
	return msgs, nil
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
		ctx:           producerCore.Ctx,
		rds:           redis.NewClient(redisOpts),
		prefix:        cfg.KeyPrefix,
		queueKey:      fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
		delayQueueKey: fmt.Sprintf("%s:delayqueue:%s", cfg.KeyPrefix, cfg.Topic),
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

func (p *RProducer) Send(data any, opts ...core.MsgOption) (string, error) {
	return p.producerCore.Send(p.client, core.NewMsg(data, opts...))
}

type RConsumer struct {
	consumerCore *core.ConsumerCore
	client       *rClient
}

// NewRConsumer returns a consumer and error
func NewRConsumer(cfg *RConf, handler core.ConsumeFunc, opts ...core.ConsumerOption) (*RConsumer, error) {
	redisOpts, err := redis.ParseURL(cfg.Url)
	if err != nil {
		return nil, err
	}
	var batchProcess *BatchProcessConf
	if cfg.BatchProcess == nil {
		batchProcess = &BatchProcessConf{}
	} else {
		batchProcess = cfg.BatchProcess
	}
	consumerCore := &core.ConsumerCore{
		Ctx:                 context.Background(),
		Topic:               cfg.Topic,
		WorkersNum:          cfg.Workers,
		Processors:          goset.NewSortedSet[core.ProcessorType](),
		ConsumeFunc:         handler,
		BatchProcessCnt:     batchProcess.Batch,
		BatchProcessTimeout: time.Duration(batchProcess.Timeout) * time.Second,
	}
	for _, opt := range opts {
		opt(consumerCore)
	}
	client := &rClient{
		ctx:           consumerCore.Ctx,
		rds:           redis.NewClient(redisOpts),
		prefix:        cfg.KeyPrefix,
		queueKey:      fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
		delayQueueKey: fmt.Sprintf("%s:delayqueue:%s", cfg.KeyPrefix, cfg.Topic),
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
