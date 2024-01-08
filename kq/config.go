package kq

type Conf struct {
	Brokers    []string `json:"brokers" yaml:"brokers"`       // kafka broker addresses
	Group      string   `json:"group" yaml:"group"`           // consumer group,only for consumer
	Topic      string   `json:"topic" yaml:"topic"`           // topic
	Processors int      `json:"processors" yaml:"processors"` // consumer count,default is the partition count of topic
	MinBytes   int      `json:"min_bytes" yaml:"min_bytes"`   // default 10K
	MaxBytes   int      `json:"max_bytes" yaml:"max_bytes"`   // default 10M
	CaFile     string   `json:",optional" yaml:"ca_file"`     // certificate file path for connecting to kafka
	Username   string   `json:",optional" yaml:"username"`    // username for connecting to kafka
	Password   string   `json:",optional" yaml:"password"`    // password for connecting to kafka
}
