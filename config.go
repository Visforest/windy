package windy

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"strings"
)

type KafkaConf struct {
	// kafka broker addresses
	Brokers []string `json:"brokers" yaml:"brokers"`

	// consumer group name
	Group string `json:"group" yaml:"group"`

	// whether to create topic if topic is missing, default false
	AutoCreateTopic *bool `json:"auto_create_topic" yaml:"auto_create_topic"`

	// the count of the topics to create, default 4
	Partitions int `json:"topic_partitions" yaml:"topic_partitions"`

	// the replication count of each topic partition, default 3
	Replications int `json:"replications" yaml:"replications"`

	// default 10K
	MinBytes int `json:"min_bytes" yaml:"min_bytes"`

	// default 10M
	MaxBytes int `json:"max_bytes" yaml:"max_bytes"`

	// certificate file path for connecting to kafka
	CaFile string `json:"ca_file" yaml:"ca_file"`

	// username for connecting to kafka
	Username string `json:"username" yaml:"username"`

	// password for connecting to kafka
	Password string `json:"password" yaml:"password"`
}

// BatchProcessConf specifies the configuration of deduplication or compress process, it'll be ignored if the DeduplicateHandler is missing
type BatchProcessConf struct {
	// the max number of msgs to be deduplicated once,default 100
	Batch int `json:"batch" yaml:"batch"`
	// the max seconds at which deduplication process will be finished,default 10
	Timeout int `json:"timeout" yaml:"timeout"`
}

// KConf is used for KProducer and KConsumer
type KConf struct {
	// topic name
	Topic string `json:"topic" yaml:"topic"`

	// the count of workers that consumes synchronously, default is the count of topic partition
	Processors int `json:"processors" yaml:"processors"`

	// kafka common
	Kafka *KafkaConf `json:"kafka" yaml:"kafka"`

	BatchProcessConf *BatchProcessConf `json:"batch_process_conf" yaml:"batch_process_conf"`
}

// RConf is used for RProducer and RConsumer
type RConf struct {
	// redis nodes address, it'll be ignored if you specify a redis.Client for Producer or Consumer
	Url string `json:"brokers" yaml:"brokers"`

	// topic
	Topic string `json:"topic" yaml:"topic"`

	// the count of workers that consumes synchronously
	Processors int `json:"processors" yaml:"processors"`

	// the prefix of redis keys used,default 'windy'
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix"`

	BatchProcessConf *BatchProcessConf `json:"batch_process_conf" yaml:"batch_process_conf"`
}

// LoadConfig loads Conf from specified file path
func LoadConfig(file string, conf any) error {
	bytes, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	ext := strings.ToLower(path.Ext(file))
	switch ext {
	case ".json":
		err = json.Unmarshal(bytes, conf)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(bytes, conf)
	default:
		err = errors.New("unsupported configuration format")
	}
	return err
}

// MustLoadConfig loads Conf from specified file path,panics on error
func MustLoadConfig(file string, conf any) {
	if err := LoadConfig(file, conf); err != nil {
		panic(err)
	}
}
