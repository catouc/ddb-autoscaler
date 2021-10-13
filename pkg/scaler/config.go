package scaler

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type Config struct {
	AWSConfig      aws.Config
	Interval       time.Duration
	ScalingTargets []*TableScalingConfig
}

func (cfg *Config) Check() error {
	if cfg.Interval < 60*time.Second {
		return errors.New("interval must be 60s or greater")
	}

	for _, t := range cfg.ScalingTargets {
		if t.ScaleDownDelay < 60*time.Minute {
			return fmt.Errorf("%s: scaleDownDelay must be 60m or greater", t.TableName)
		}
	}

	return nil
}

type TableScalingConfig struct {
	TableName string

	ReadUpperBound      float64
	WriteUpperBound     float64
	ReadLowerBound      float64
	WriteLowerBound     float64
	ReadBufferCapacity  float64
	WriteBufferCapacity float64

	ScaleDownDelay time.Duration
	lastScaleTime  time.Time
}

func (tsc *TableScalingConfig) String() string {
	tscBytes, err := json.Marshal(*tsc)
	if err != nil {
		return ""
	}

	return string(tscBytes)
}
