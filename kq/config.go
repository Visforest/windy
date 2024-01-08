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
	Brokers    []string `json:"brokers" yaml:"brokers"`       // kafka broker addresses
	Group      string   `json:"group" yaml:"group"`           // consumer group,only for consumer
	Topic      string   `json:"topic" yaml:"topic"`           // topic
	Processors int      `json:"processors" yaml:"processors"` // consumer count,default is the partition count of topic
	MinBytes   int      `json:"min_bytes" yaml:"min_bytes"`   // default 10K
	MaxBytes   int      `json:"max_bytes" yaml:"max_bytes"`   // default 10M
	CaFile     string   `json:"ca_file" yaml:"ca_file"`       // certificate file path for connecting to kafka
	Username   string   `json:"username" yaml:"username"`     // username for connecting to kafka
	Password   string   `json:"password" yaml:"password"`     // password for connecting to kafka
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
