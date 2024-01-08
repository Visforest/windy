[English](README.md) | 中文

# windy
一个消息/任务队列异步处理库，目前仅支持 Kafka 作为中间件。

支持：
1. 消息发送前、发送成功、发送失败时回调自定义的函数。
2. 自定义消息 ID 生成器。
3. 使用 Context，以传递和使用元数据。

其他特性后续会增加。

#  使用方法

安装:
```
go get github.com/visforest/windy
```

Producer:
```go
func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
        Group:   "g.notify.email",
	}
	// pass metadata
	ctx := context.WithValue(context.Background(), "channel", "pc")
	// initialize a producer
	// optionally specify your listener and id creator
	producer := kq.MustNewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&myProduceListener{}), kq.WithIdCreator(&myIdCreator{}))
    
	// prepare and send msgs
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
```

你可以定义 listener 和 id creator, 详见 [example/producer.go](example/producer.go).

Consumer:
```go
func sendEmail(ctx context.Context, topic string, msg *model.Msg) error {
	ip := ctx.Value("myip").(string)
	fmt.Printf("start to send email from ip: %s,topic is %s \n", ip, topic)
	if data, ok := msg.Data.(map[string]interface{}); ok {
		receiver, ok := data["receiver"]
		if !ok {
			return errors.New("got bad data from queue")
		}
		content, ok := data["content"]
		if !ok {
			return errors.New("got bad data from queue")
		}
		fmt.Printf("send to %s:%s \n", receiver, content)
		return nil
	}
	return errors.New("got bad data from queue")
}

func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	// pass metadata
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	// initialize a consumer
	consumer := kq.MustNewConsumer(&cfg, sendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&myConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}
```
你可以定义 listener, 详见 [example/consumer.go](example/consumer.go).