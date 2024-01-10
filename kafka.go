package windy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/visforest/windy/core"
	"os"
	"strings"
	"time"
)

type kClient struct {
	ctx    context.Context
	conn   *kafka.Conn
	writer *kafka.Writer
	reader *kafka.Reader
}

func (c *kClient) Push(m *core.Msg) error {
	val, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.writer.WriteMessages(c.ctx, kafka.Message{Key: []byte(m.Id), Value: val})
}

func (c *kClient) Fetch() (*core.Msg, error) {
	message, err := c.reader.FetchMessage(c.ctx)
	if err != nil {
		return nil, err
	}
	var m core.Msg
	if err = json.Unmarshal(message.Value, &m); err == nil {
		return &m, nil
	}
	return nil, err
}

type KProducer struct {
	producerCore *core.ProducerCore
	client       *kClient
}

// NewKProducer returns a producer and an error
func NewKProducer(cfg *KConf, opts ...core.ProducerOption) (*KProducer, error) {
	conn, err := kafka.Dial("tcp", cfg.Kafka.Brokers[0])
	if err != nil {
		return nil, err
	}
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Kafka.Brokers...),
		Topic:                  cfg.Topic,
		AllowAutoTopicCreation: *cfg.Kafka.AutoCreateTopic,
		Balancer:               &kafka.LeastBytes{},
		Compression:            kafka.Snappy,
	}
	if *cfg.Kafka.AutoCreateTopic {
		// although writer can create topic if missing, but partitions count and replications count are important for efficiency,
		// but writer doesn't ensure that
		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.Kafka.Partitions,
			ReplicationFactor: cfg.Kafka.Replications,
		})
		if err != nil {
			return nil, err
		}
	}
	producerCore := &core.ProducerCore{
		Ctx:       context.Background(),
		Topic:     cfg.Topic,
		IdCreator: core.NewSnowflakeCreator(1),
	}
	for _, opt := range opts {
		opt(producerCore)
	}
	client := &kClient{
		conn:   conn,
		writer: writer,
		reader: nil,
	}
	return &KProducer{
		producerCore: producerCore,
		client:       client,
	}, nil
}

// MustNewKProducer returns a producer or panic if fails
func MustNewKProducer(cfg *KConf, opts ...core.ProducerOption) *KProducer {
	producer, err := NewKProducer(cfg, opts...)
	if err != nil {
		panic(err)
	}
	return producer
}

// Send sends data to message queue
func (p *KProducer) Send(data any) (string, error) {
	return p.producerCore.Send(p.client, core.NewMsg(data))
}

type KConsumer struct {
	consumerCore *core.ConsumerCore
	client       *kClient
}

// NewKConsumer returns a consumer and error
func NewKConsumer(cfg *KConf, ConsumeFunc core.ConsumeFunc, opts ...core.ConsumerOption) (*KConsumer, error) {
	if len(cfg.Kafka.Brokers) < 1 {
		return nil, fmt.Errorf("brokers must not be empty, at least one is required")
	}
	if strings.TrimSpace(cfg.Kafka.Group) == "" {
		return nil, fmt.Errorf("consumer group name must not be empty")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}
	if cfg.Kafka.MinBytes <= 0 {
		cfg.Kafka.MinBytes = 10 * 1 << 10
	}
	if cfg.Kafka.MaxBytes <= 0 {
		cfg.Kafka.MaxBytes = 10 * 1 << 20
	}
	readerConfig := kafka.ReaderConfig{
		Brokers:     cfg.Kafka.Brokers,
		GroupID:     cfg.Kafka.Group,
		Topic:       cfg.Topic,
		MinBytes:    cfg.Kafka.MinBytes,
		MaxBytes:    cfg.Kafka.MaxBytes,
		StartOffset: kafka.FirstOffset,
	}
	if len(cfg.Kafka.Username) > 0 && len(cfg.Kafka.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: cfg.Kafka.Username,
				Password: cfg.Kafka.Password,
			},
		}
	}
	if len(cfg.Kafka.CaFile) > 0 {
		caCert, err := os.ReadFile(cfg.Kafka.CaFile)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return nil, errors.New("certificate file is invalid")
		}

		readerConfig.Dialer.TLS = &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
	}
	// connect and get partitions count
	conn, err := kafka.Dial("tcp", cfg.Kafka.Brokers[0])
	if err != nil {
		return nil, err
	}
	partitions, err := conn.ReadPartitions(cfg.Topic)
	if err != nil {
		return nil, err
	}
	if cfg.Processors <= 0 {
		// set topic partition count or 1 as default consumer count
		cfg.Processors = len(partitions)
	} else if cfg.Processors < len(partitions) {
		// warning, it's not the best practice
	}
	reader := kafka.NewReader(readerConfig)
	consumerCore := &core.ConsumerCore{
		Ctx:                 context.Background(),
		ConsumeFunc:         ConsumeFunc,
		Processors:          cfg.Processors,
		Topic:               cfg.Topic,
		BatchProcessCnt:     cfg.BatchProcessConf.Batch,
		BatchProcessTimeout: time.Duration(cfg.BatchProcessConf.Timeout) * time.Second,
	}
	for _, opt := range opts {
		opt(consumerCore)
	}

	client := &kClient{
		ctx:    consumerCore.Ctx,
		conn:   conn,
		writer: nil,
		reader: reader,
	}
	return &KConsumer{
		consumerCore: consumerCore,
		client:       client,
	}, nil
}

// MustNewKConsumer returns a consumer, if it fails, panic
func MustNewKConsumer(cfg *KConf, ConsumeFunc core.ConsumeFunc, opts ...core.ConsumerOption) *KConsumer {
	consumer, err := NewKConsumer(cfg, ConsumeFunc, opts...)
	if err != nil {
		panic(err)
	}
	return consumer
}

// LoopConsume blocks and consumes msgs in loop with multi goroutine
func (c *KConsumer) LoopConsume() {
	c.consumerCore.LoopConsume(c.client)
}