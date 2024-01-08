package kq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/visforest/windy/model"
	"github.com/visforest/windy/plugins"
)

type ConsumeHandler func(ctx context.Context, topic string, msg *model.Msg) error

type Consumer struct {
	ctx      context.Context
	conn     *kafka.Conn
	reader   *kafka.Reader
	topic    string
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
	if len(cfg.Brokers) < 1 {
		return nil, fmt.Errorf("brokers must not be empty, at least one is required")
	}
	if strings.TrimSpace(cfg.Group) == "" {
		return nil, fmt.Errorf("consumer group name must not be empty")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 10 * 1 << 10
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 * 1 << 20
	}
	readerConfig := kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.Group,
		Topic:       cfg.Topic,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		StartOffset: kafka.FirstOffset,
	}
	if len(cfg.Username) > 0 && len(cfg.Password) > 0 {
		readerConfig.Dialer = &kafka.Dialer{
			SASLMechanism: plain.Mechanism{
				Username: cfg.Username,
				Password: cfg.Password,
			},
		}
	}
	if len(cfg.CaFile) > 0 {
		caCert, err := os.ReadFile(cfg.CaFile)
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
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
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
	consumer := &Consumer{
		conn:       conn,
		reader:     reader,
		topic:      cfg.Topic,
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

// Consume consumes once in single goroutine
func (c *Consumer) Consume() error {
	var err error
	var message kafka.Message
	if c.listener == nil {
		// before consume
		message, err = c.reader.ReadMessage(c.ctx)
		if err != nil {
			return err
		}
		var msg model.Msg
		if err := json.Unmarshal(message.Value, &msg); err != nil {
			return err
		}
		// consume
		err = c.handler(c.ctx, c.topic, &msg)
	} else {
		// before consume
		message, err = c.reader.ReadMessage(c.ctx)
		if err != nil {
			c.listener.PrepareConsume(c.ctx, c.topic, nil, err)
			return err
		}
		var msg model.Msg
		if err := json.Unmarshal(message.Value, &msg); err != nil {
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
	var wg sync.WaitGroup
	for i := 0; i < c.processors; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if err := c.Consume(); err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
						break
					}
					continue
				}
			}
		}()
	}
	wg.Wait()
}
