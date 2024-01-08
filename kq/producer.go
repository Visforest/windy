package kq

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/visforest/windy/model"
	"github.com/visforest/windy/plugins"
)

type Producer struct {
	ctx       context.Context
	conn      *kafka.Conn
	writer    *kafka.Writer
	topic     string
	idCreator plugins.IdCreator
	listener  plugins.ProducerListener
}

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

// NewProducer returns a producer and an error
func NewProducer(cfg *Conf, opts ...ProducerOption) (*Producer, error) {
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return nil, err
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		AllowAutoTopicCreation: *cfg.AutoCreateTopic,
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Snappy,
	}
	if *cfg.AutoCreateTopic {
		// although writer can create topic if missing, but partitions count and replications count are important for efficiency,
		// but writer doesn't ensure that
		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.Partitions,
			ReplicationFactor: cfg.Replications,
		})
		if err != nil {
			return nil, err
		}
	}
	producer := &Producer{
		ctx:       context.Background(),
		conn:      conn,
		writer:    writer,
		topic:     cfg.Topic,
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
	msg := kafka.Message{
		Key:   []byte(m.Id),
		Value: val,
	}

	if p.listener == nil {
		err = p.writer.WriteMessages(p.ctx, msg)
	} else {
		// before send
		p.listener.PrepareSend(p.ctx, p.topic, m, nil)
		// send
		err = p.writer.WriteMessages(p.ctx, msg)
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
