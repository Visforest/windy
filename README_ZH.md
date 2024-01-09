[English](README.md) | 中文

# windy
一个消息/任务队列异步处理库，目前有 Kafka 的 `kq` 和 Redis 的 `rq` 可供选择。

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

下面是 `kq` 的示例，`rq` 的用法与其相同。

Producer:
```go
func main() {
	cfg := kq.Conf{
		Brokers: []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:   "notify.email",
		Group:   "g.notify.email",
	}
	ctx := context.WithValue(context.Background(), "channel", "pc")
	producer := kq.MustNewProducer(&cfg, kq.WithProducerContext(ctx), kq.WithProducerListener(&example.MyProduceListener{}), kq.WithIdCreator(&example.MyIdCreator{}))

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

Consumer:
```go
func main() {
	cfg := kq.Conf{
		Brokers:    []string{"master:9092", "node1:9092", "node2:9092"},
		Topic:      "notify.email",
		Group:      "g.notify.email",
		Processors: 4,
	}
	ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
	consumer := kq.MustNewConsumer(&cfg, example.SendEmail, kq.WithConsumerContext(ctx), kq.WithConsumerListener(&example.MyConsumerListener{}))
	// block to consume
	consumer.LoopConsume()
}
```


你可以定义生产者、消费者 listener，消息 ID 生成器，以及消息处理函数, 可参考 [example/utils.go](example/utils.go).