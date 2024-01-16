[English](README.md) | 中文

# windy
一个消息/任务队列异步处理库，目前有 Kafka 的 `kq` 和 Redis 的 `rq` 可供选择。

支持：
1. 消息发送前、发送成功、发送失败时回调自定义的函数。
2. 自定义消息 ID 生成器。
3. 使用 Context，以传递和使用元数据。
4. json、yaml 格式配置文件
5. 压缩消息
6. 解压消息
7. 消息去重

其他特性后续会增加。

#  使用方法

安装:
```
go get github.com/visforest/windy
```

下面是 `kq` 的示例，`rq` 的用法与其相同。


## Producer

### 常规用法

context，ID 生成器，ProducerListener 可选，可根据需要定制。

```go
var cfg windy.RConf
windy.MustLoadConfig("config.yaml", &cfg)
ctx := context.WithValue(context.Background(), "channel", "pc")
producer := windy.MustNewRProducer(&cfg, core.WithProducerContext(ctx), core.WithProducerListener(&example.MyProduceListener{}))

for _, e := range example.Emails {
	msgId, err := producer.Send(e)
	if err != nil {
		panic(err)
	}
	fmt.Printf("send msg %s \n", msgId)
}
```
## Consumer

### context,listener

context 可以携带元数据，listener 能够在消费前、消费成功时、消费失败时回调，这样就可以记录日志或处理一些逻辑。

```go
var cfg windy.RConf
windy.MustLoadConfig("config.yaml", &cfg)
ctx := context.WithValue(context.Background(), "myip", "10.0.10.1")
consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithConsumerContext(ctx), core.WithConsumerListener(&example.MyConsumerListener{}))
fmt.Println("start to consume")
// block to consume
consumer.LoopConsume()
```

### 压缩并消费

消息可以被压缩。例如发给不同人的相同的邮件，可以被压缩为一个群发邮件。

```go
// returns msgs grouped by Email.Subject and Email.Content
func compress(msgs []*core.Msg) []*core.Msg {
	group := make(map[string]*goset.Set)
	group2 := make(map[string]*core.Msg)
	for _, msg := range msgs {
		var email example.Email
		if err := core.ParseFromMsg(msg, &email); err != nil {
			fmt.Printf("%v \n", err)
			continue
		}
		key := fmt.Sprintf("%s_%s", email.Subject, email.Content)
		if email.Receiver != "" {
			email.Receivers = append(email.Receivers, email.Receiver)
		}
		if _, ok := group[key]; !ok {
			group[key] = goset.NewSet()
		}
		for _, r := range email.Receivers {
			group[key].Add(r)
		}
		if _, ok := group2[key]; !ok {
			group2[key] = &core.Msg{
				Id: msg.Id,
				Data: example.Email{
					Subject: email.Subject,
					Content: email.Content,
				},
			}
		}
	}
	var result = make([]*core.Msg, 0, len(group))
	for key, receiverSet := range group {
		// get receivers
		receivers := make([]string, 0, receiverSet.Length())
		for _, r := range receiverSet.ToList() {
			receivers = append(receivers, r.(string))
		}
		// get email
		msg := group2[key]
		email := msg.Data.(example.Email)
		email.Receivers = receivers
		msg.Data = email
		result = append(result, msg)
	}
	return result
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.BatchSendEmail, core.WithCompressFunc(compress))
	consumer.LoopConsume()
}
```

### 解压并消费

有时一条消息需要扩展为多条，示例代码：
```go

// defines the receivers of email should be single
func decompress(msg *core.Msg) []*core.Msg {
	var msgs []*core.Msg
	var email example.Email
	if err := core.ParseFromMsg(msg, &email); err != nil {
		panic(err)
	}
	if len(email.Receivers) > 0 {
		// multi receivers
		msgs = make([]*core.Msg, len(email.Receivers))
		for i, receiver := range email.Receivers {
			msgs[i] = core.NewMsg(example.Email{
				Receiver: receiver,
				Subject:  email.Subject,
				Content:  email.Content,
			})
		}
	} else {
		// single receiver
		msgs = []*core.Msg{msg}
	}
	return msgs
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithDecompressFunc(decompress))
	consumer.LoopConsume()
}
```

### 去重并消费

示例：
```go
// defines the combination of receiver and subject must be unique
func uniq(msg *core.Msg) string {
	var data example.Email
	if err := core.ParseFromMsg(msg, &data); err == nil {
		subject := data.Subject
		var receiver string
		if len(data.Receivers) > 0 {
			receiver = strings.Join(data.Receivers, ";")
		} else {
			receiver = data.Receiver
		}
		return fmt.Sprintf("%s:%s", receiver, subject)
	}
	return ""
}

func main() {
	var cfg windy.RConf
	windy.MustLoadConfig("config.yaml", &cfg)
	consumer := windy.MustNewRConsumer(&cfg, example.SendEmail, core.WithUniqFunc(uniq))
	consumer.LoopConsume()
}
```
---

你可以定义生产者、消费者 listener，消息 ID 生成器，以及消息处理函数, 可参考 [example/utils.go](example/utils.go).