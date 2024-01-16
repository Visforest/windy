package windy

import (
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
)

type KafkaConf struct {
	// kafka broker addresses
	Brokers []string `json:"brokers,min_len=1" yaml:"brokers,min_len=1"`

	// consumer group name
	Group string `json:"group,required=true" yaml:"group,required=true"`

	// whether to create topic if topic is missing, default false
	AutoCreateTopic bool `json:"auto_create_topic,default=false" yaml:"auto_create_topic,default=false"`

	// the count of the topics to create, default 4
	Partitions int `json:"topic_partitions,default=4" yaml:"topic_partitions,default=4"`

	// the replication count of each topic partition, default 3
	Replications int `json:"replications,default=3,min=1" yaml:"replications,default=3,min=1"`

	// default 10K
	MinBytes int `json:"min_bytes,default=10240,min=1024" yaml:"min_bytes,default=10240,min=1024"`

	// default 10M, must be greater than MinBytes
	MaxBytes int `json:"max_bytes,default=10485760,gtf=min_bytes" yaml:"max_bytes,default=10485760,gtf=min_bytes"`

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
	Batch int `json:"batch" yaml:"batch" validate:"default=100"`

	// the max seconds at which deduplication process will be finished,default 10
	Timeout int `json:"timeout" yaml:"timeout" validate:"default=10,min=1"`
}

// KConf is configuration for KProducer and KConsumer
type KConf struct {
	// topic name
	Topic string `json:"topic" yaml:"topic" validate:"required=true"`

	// the count of workers that consumes synchronously, default is the count of topic partition
	Processors int `json:"processors" yaml:"processors" validate:"min=1"`

	// kafka configuration
	Kafka *KafkaConf `json:"kafka" yaml:"kafka" validate:"required=true"`

	// the configuration for batch processing, such as msg compression, msg deduplication
	BatchProcess *BatchProcessConf `json:"batch_process" yaml:"batch_process"`
}

// RConf is configuration for RProducer and RConsumer
type RConf struct {
	// redis nodes address
	// It'll be ignored if you specify a redis.Client for Producer or Consumer
	Url string `json:"url" yaml:"url" validate:"required=true"`

	// topic
	Topic string `json:"topic" yaml:"topic" validate:"required=true"`

	// the count of workers that consumes synchronously,default 4
	Processors int `json:"processors" yaml:"processors" validate:"default=4"`

	// the prefix of redis keys used,default 'windy'
	KeyPrefix string `json:"key_prefix" yaml:"key_prefix" validate:"default=windy"`

	// the configuration for batch processing, such as msg compression, msg deduplication
	BatchProcess *BatchProcessConf `json:"batch_process" yaml:"batch_process"`
}

const (
	tagDefault  = "default"  // set default value
	tagRequired = "required" // restrict if value must be set and not empty
	tagMin      = "min"      // restrict min value
	tagMax      = "max"      // restrict max value
	tagGtf      = "gtf"      // restrict value greater than specified field value
	tagMinLen   = "min_len"  // restrict min length of string,slice,map
	tagMaxLen   = "max_len"  // restrict max length of string,slice,map
)

func getTagValue(content string) map[string]string {
	result := make(map[string]string)
	if content == "" {
		return result
	}
	for _, part := range strings.Split(content, ",") {
		s := strings.Split(part, "=")
		result[strings.TrimSpace(s[0])] = strings.TrimSpace(s[1])
	}
	return result
}

func validate(conf any) error {
	if reflect.ValueOf(conf).Kind() != reflect.Pointer {
		return errors.New("conf must be a pointer to struct")
	}
	typ := reflect.TypeOf(conf).Elem()
	val := reflect.ValueOf(conf).Elem()

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldVal := val.Field(i)
		tagVals := getTagValue(field.Tag.Get("validate"))

		if fieldVal.IsZero() {
			if fieldVal.Kind() == reflect.Pointer {
				// skip nil pointer
				continue
			}
			// set default value for zero value field
			if defaultValStr, ok := tagVals[tagDefault]; ok {
				switch fieldVal.Kind() {
				case reflect.String:
					fieldVal.SetString(defaultValStr)
				case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
					v, err := strconv.ParseInt(defaultValStr, 10, 64)
					if err != nil {
						return err
					}
					fieldVal.SetInt(v)
				case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
					v, err := strconv.ParseUint(defaultValStr, 10, 64)
					if err != nil {
						return err
					}
					fieldVal.SetUint(v)
				case reflect.Float32, reflect.Float64:
					v, err := strconv.ParseFloat(defaultValStr, 64)
					if err != nil {
						return err
					}
					fieldVal.SetFloat(v)
				}
			} else {
				if requiredStr, ok := tagVals[tagRequired]; ok {
					required, err := strconv.ParseBool(requiredStr)
					if err != nil {
						return err
					}
					if required {
						// field value is required, but not set
						return fmt.Errorf("filed '%s' must not be empty or zero value", field.Name)
					}
				}
			}
		} else {
			switch fieldVal.Kind() {
			case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
				v := fieldVal.Int()
				if minValStr, ok := tagVals[tagMin]; ok {
					minVal, err := strconv.ParseInt(minValStr, 10, 64)
					if err != nil {
						return err
					}
					if v < minVal {
						return fmt.Errorf("field '%s' must be greater than or equal to %d", field.Name, minVal)
					}
				}
				if maxValStr, ok := tagVals[tagMax]; ok {
					maxVal, err := strconv.ParseInt(maxValStr, 10, 64)
					if err != nil {
						return err
					}
					if v > maxVal {
						return fmt.Errorf("field '%s' must be less than or equal to %d", field.Name, maxVal)
					}
				}
				if greaterThanField, ok := tagVals[tagGtf]; ok {
					f := val.FieldByName(greaterThanField)
					if v < f.Int() {
						return fmt.Errorf("field '%s' must be greater than '%s'", field.Name, f.String())
					}
				}
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
				v := fieldVal.Uint()
				if minValStr, ok := tagVals[tagMin]; ok {
					minVal, err := strconv.ParseUint(minValStr, 10, 64)
					if err != nil {
						return err
					}
					if v < minVal {
						return fmt.Errorf("%s must be greater than or equal to %d", field.Name, minVal)
					}
				}
				if maxValStr, ok := tagVals[tagMax]; ok {
					maxVal, err := strconv.ParseUint(maxValStr, 10, 64)
					if err != nil {
						return err
					}
					if v > maxVal {
						return fmt.Errorf("%s must be less than or equal to %d", field.Name, maxVal)
					}
				}
				if greaterThanField, ok := tagVals[tagGtf]; ok {
					f := val.FieldByName(greaterThanField)
					if v <= f.Uint() {
						return fmt.Errorf("field '%s' must be greater than '%s'", field.Name, f.String())
					}
				}
			case reflect.Float32, reflect.Float64:
				v := fieldVal.Float()
				if minValStr, ok := tagVals[tagMin]; ok {
					minVal, err := strconv.ParseFloat(minValStr, 64)
					if err != nil {
						return err
					}
					if v < minVal {
						return fmt.Errorf("field '%s' must be greater than or equal to %f", field.Name, minVal)
					}
				}
				if maxValStr, ok := tagVals[tagMax]; ok {
					maxVal, err := strconv.ParseFloat(maxValStr, 64)
					if err != nil {
						return err
					}
					if v > maxVal {
						return fmt.Errorf("field '%s' must be less than or equal to %f", field.Name, maxVal)
					}
				}
				if greaterThanField, ok := tagVals[tagGtf]; ok {
					f := val.FieldByName(greaterThanField)
					if v <= f.Float() {
						return fmt.Errorf("field '%s' must be greater than '%s'", field.Name, f.String())
					}
				}
			case reflect.String, reflect.Slice, reflect.Array, reflect.Map:
				if minLenStr, ok := tagVals[tagMinLen]; ok {
					minLen, err := strconv.ParseInt(minLenStr, 10, 64)
					if err != nil {
						return err
					}
					if int64(fieldVal.Len()) < minLen {
						return fmt.Errorf("field '%s' length must be greater than or equal to %d", field.Name, minLen)
					}
				}
				if maxLenStr, ok := tagVals[tagMaxLen]; ok {
					maxLen, err := strconv.ParseInt(maxLenStr, 10, 64)
					if err != nil {
						return err
					}
					if int64(fieldVal.Len()) > maxLen {
						return fmt.Errorf("field '%s' length must be less than or equal to %d", field.Name, maxLen)
					}
				}
			case reflect.Pointer:
				if err := validate(fieldVal.Interface()); err != nil {
					return err
				}
			}
		}
	}

	return nil
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
	if err != nil {
		return err
	}

	return validate(conf)
}

// MustLoadConfig loads Conf from specified file path,panics on error
func MustLoadConfig(file string, conf any) {
	if err := LoadConfig(file, conf); err != nil {
		panic(err)
	}
}
