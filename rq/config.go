package rq

type Conf struct {
	// redis nodes address, it'll be ignored if you specify a redis.Client for Producer or Consumer
	Url string `json:"brokers" yaml:"brokers"`

	// topic
	Topic string `json:"topic" yaml:"topic"`

	// the prefix of redis keys used,default 'windy'
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix"`

	// the count of workers that consumes synchronously
	Processors int `json:"processors" yaml:"processors"`
}
