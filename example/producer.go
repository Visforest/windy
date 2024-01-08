package main

import (
	"context"
	"fmt"
	"github.com/visforest/windy/kq"
	"github.com/visforest/windy/model"
	"math/rand"
	"strings"
)

type myProduceListener struct{}

func (l *myProduceListener) PrepareSend(ctx context.Context, topic string, msg *model.Msg, err error) {
	if err != nil {
		fmt.Printf("get err before msg is sent:%s \n", err.Error())
		return
	}
	channel := ctx.Value("channel").(string)
	fmt.Printf("sending msg %s to %s,channel: %s\n", msg.Id, topic, channel)
}

func (l *myProduceListener) OnSendSucceed(ctx context.Context, topic string, msg *model.Msg) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("sent msg %s to %s,channel: %s \n", msg.Id, topic, channel)
}

func (l *myProduceListener) OnSendFail(ctx context.Context, topic string, msg *model.Msg, err error) {
	channel := ctx.Value("channel").(string)
	fmt.Printf("failed to send msg %s to %s, %s,channel: %s \n", msg.Id, topic, err.Error(), channel)
}

// customized id creator
type myIdCreator struct{}

func (c *myIdCreator) Create() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 10)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := kq.MustNewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&myProduceListener{}), kq.WithIdCreator(&myIdCreator{}))

	receivers := []string{"wind@example.com", "cloud@example.com", "rain@example.com", "snow@example.com", "storm@example.com"}
	for _, r := range receivers {
		data := map[string]string{"receiver": r, "content": fmt.Sprintf("Hi, %s!", strings.TrimRight(r, "@example.com"))}
		msgId, err := producer.Send(data)
		if err != nil {
			panic(err)
		}
		fmt.Printf("send msg %s to %s \n", msgId, r)
	}
}
