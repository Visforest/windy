package core

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"reflect"
)

type Msg struct {
	Id   string `json:"id"`
	Data any    `json:"data"`
}

func NewMsg(data any) *Msg {
	return &Msg{Data: data}
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
