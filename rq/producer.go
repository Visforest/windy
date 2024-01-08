package rq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/visforest/windy/model"
	"github.com/visforest/windy/plugins"
)

type ProducerOption func(producer *Producer)

func WithProducerContext(ctx context.Context) ProducerOption {
	return func(p *Producer) {
		p.ctx = ctx
	}
}

func WithIdCreator(creator plugins.IdCreator) ProducerOption {
	return func(p *Producer) {
		p.idCreator = creator
	}
}

func WithProducerListener(listener plugins.ProducerListener) ProducerOption {
	return func(p *Producer) {
		p.listener = listener
	}
}

type Producer struct {
	ctx       context.Context
	client    *redis.Client
	topic     string
	prefix    string
	queueKey  string
	idCreator plugins.IdCreator
	listener  plugins.ProducerListener
}

// NewProducer returns a producer and an error
func NewProducer(cfg *Conf, opts ...ProducerOption) (*Producer, error) {
	redisOpts, err := redis.ParseURL(cfg.Url)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(redisOpts)
	producer := &Producer{
		ctx:       context.Background(),
		client:    client,
		topic:     cfg.Topic,
		prefix:    cfg.KeyPrefix,
		queueKey:  fmt.Sprintf("%s:queue:%s", cfg.KeyPrefix, cfg.Topic),
		idCreator: plugins.NewSnowflakeCreator(1),
	}
	for _, opt := range opts {
		opt(producer)
	}
	return producer, nil
}

// MustNewProducer returns a producer or panic if fails
func MustNewProducer(cfg *Conf, opts ...ProducerOption) *Producer {
	producer, err := NewProducer(cfg, opts...)
	if err != nil {
		panic(err)
	}
	return producer
}

func (p *Producer) push(m *model.Msg) (string, error) {
	// generate msg id
	m.Id = p.idCreator.Create()
	val, err := json.Marshal(m)
	if err != nil {
		if p.listener != nil {
			p.listener.PrepareSend(p.ctx, p.topic, m, err)
		}
		return "", err
	}
	msg := string(val)

	if p.listener == nil {
		err = p.client.LPush(p.ctx, p.queueKey, msg).Err()
	} else {
		// before send
		p.listener.PrepareSend(p.ctx, p.topic, m, nil)
		// send
		err = p.client.LPush(p.ctx, p.queueKey, msg).Err()
		if err != nil {
			// on send fail
			p.listener.OnSendFail(p.ctx, p.topic, m, err)
			return "", err
		}
		// after send
		p.listener.OnSendSucceed(p.ctx, p.topic, m)
	}
	return m.Id, err
}

// Send sends data to message queue, the data must be Json serializable
func (p *Producer) Send(data any) (string, error) {
	return p.push(model.NewMsg(data))
}
