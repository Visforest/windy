package kq

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"strings"
)

type Conf struct {
	// kafka broker addresses
	Brokers []string `json:"brokers" yaml:"brokers"`

	// topic name
	Topic string `json:"topic" yaml:"topic"`

	// consumer group name
	Group string `json:"group" yaml:"group"`

	// whether to create topic if topic is missing, default false
	AutoCreateTopic *bool `json:"auto_create_topic" yaml:"auto_create_topic"`

	// the count of the topics to create, default 4
	Partitions int `json:"topic_partitions" yaml:"topic_partitions"`

	// the replication count of each topic partition, default 3
	Replications int `json:"replications" yaml:"replications"`

	// the count of workers that consumes synchronously, default is the count of topic partition
	Processors int `json:"processors" yaml:"processors"`

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

// LoadConfig loads Conf from specified file path
func LoadConfig(file string) (*Conf, error) {
	var conf Conf
	bytes, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(path.Ext(file))
	switch ext {
	case ".json":
		err = json.Unmarshal(bytes, &conf)
	case ".yaml", ".yml":
		err = yaml.Unmarshal(bytes, &conf)
	default:
		err = errors.New("unsupported configuration format")
	}
	if err == nil {
		if len(conf.Brokers) == 0 {
			return nil, errors.New("brokers must bot be empty")
		}
		if strings.TrimSpace(conf.Topic) == "" {
			return nil, errors.New("topic must not be empty")
		}
		if strings.TrimSpace(conf.Group) == "" {
			return nil, errors.New("group must not be empty")
		}
		if conf.AutoCreateTopic == nil {
			*conf.AutoCreateTopic = false
		} else if *conf.AutoCreateTopic == true {
			if conf.Partitions == 0 {
				conf.Partitions = 4
			}
			if conf.Replications == 0 {
				conf.Replications = 3
			}
		}
		if conf.MinBytes == 0 {
			conf.MinBytes = 10 * 1 << 10
		}
		if conf.MaxBytes == 0 {
			conf.MaxBytes = 10 * 1 << 20
		}
	}
	return &conf, err
}

// MustLoadConfig loads Conf from specified file path,panics on error
func MustLoadConfig(file string) *Conf {
	conf, err := LoadConfig(file)
	if err != nil {
		panic(err)
	}
	return conf
}
