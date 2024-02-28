package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strings"
	"time"
)

type Msg struct {
	Id       string     `json:"id"`                  // msg uniq id
	DelayAt  *time.Time `json:"delay_at,omitempty"`  // the time at which the msg will be processed at, it must be later than now
	ExpireAt *time.Time `json:"expire_at,omitempty"` // the time at which the msg will expire
	Data     any        `json:"data"`                // data that will be transferred
}

type MsgOption func(m *Msg)

func WithDelayTime(delayAt *time.Time) MsgOption {
	return func(m *Msg) {
		if !delayAt.After(time.Now()) {
			panic("delay time must be later than now")
		}
		m.DelayAt = delayAt
	}
}

func WithExpireTime(expireAt *time.Time) MsgOption {
	return func(m *Msg) {
		if m.DelayAt != nil && !expireAt.After(*m.DelayAt) {
			panic("expire time must be later than delay time")
		}
		if !expireAt.After(time.Now()) {
			panic("delay time must be later than now")
		}
	}
}

func NewMsg(data any, opts ...MsgOption) *Msg {
	msg := &Msg{Data: data}
	for _, opt := range opts {
		opt(msg)
	}
	return msg
}

// ParseFromMsg decodes m.Data into s, and s must be a pointer
func ParseFromMsg(m *Msg, s interface{}) error {
	targetValue := reflect.ValueOf(s)
	if targetValue.Kind() != reflect.Ptr {
		return fmt.Errorf("incompatible types: %T and %T", s, m.Data)
	}
	// Handle different types of Data
	switch data := m.Data.(type) {
	case map[string]interface{}:
		// If Data is a map, decode it into s
		return mapstructure.Decode(m.Data, s)
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		// If Data is a basic type, assign it directly to s
		targetValue.Elem().Set(reflect.ValueOf(m.Data))
		return nil
	default:
		// If Data is a struct, struct pointer, or slice, handle it accordingly
		config := &mapstructure.DecoderConfig{
			TagName: "json",
			Result:  s,
		}
		decoder, err := mapstructure.NewDecoder(config)
		if err != nil {
			return err
		}
		return decoder.Decode(data)
	}
}

func DecodeMsgFromBytes(data []byte) (*Msg, error) {
	var m Msg
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(&m)
	return &m, err
}

func DecodeMsgFromStr(data string) (*Msg, error) {
	var m Msg
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(&m)
	return &m, err
}
