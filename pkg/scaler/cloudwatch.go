package scaler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

const (
	DDBCWNamespace                = "AWS/DynamoDB"
	DDBReadThrottleEvents         = "ReadThrottleEvents"
	DDBWriteThrottleEvents        = "WriteThrottleEvents"
	DDBConsumedReadCapacityUnits  = "ConsumedReadCapacityUnits"
	DDBConsumedWriteCapacityUnits = "ConsumedWriteCapacityUnits"
)

type tableCapacity struct {
	ReadCapacity  capacity
	WriteCapacity capacity
}

type capacity struct {
	CapType     capacityType
	Consumed    *float64
	Provisioned *int64
	Throttles   float64
}

func (tc *tableCapacity) String() string {
	tcBytes, err := json.Marshal(*tc)
	if err != nil {
		return ""
	}

	return string(tcBytes)
}

func (c *capacity) String() string {
	cBytes, err := json.Marshal(*c)
	if err != nil {
		return ""
	}

	return string(cBytes)
}

func (tc *tableCapacity) isSafe() bool {
	if tc.ReadCapacity.Consumed == nil {
		return false
	}

	if tc.ReadCapacity.Provisioned == nil {
		return false
	}

	if tc.WriteCapacity.Consumed == nil {
		return false
	}

	if tc.WriteCapacity.Provisioned == nil {
		return false
	}

	return true
}

func (s *Scaler) getTableCapacity(ctx context.Context, tableName string) (*tableCapacity, error) {
	metrics, err := s.getCWMetrics(ctx, tableName, DDBConsumedReadCapacityUnits,
		DDBConsumedWriteCapacityUnits, DDBReadThrottleEvents, DDBWriteThrottleEvents)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metrics for %s: %w", tableName, err)
	}

	c := tableCapacity{
		ReadCapacity: capacity{
			CapType: readCapacity,
		},
		WriteCapacity: capacity{
			CapType: writeCapacity,
		},
	}

	for _, mdr := range metrics.MetricDataResults {
		switch *mdr.Id {
		case strings.ToLower(DDBConsumedReadCapacityUnits):
			if len(mdr.Values) >= 1 {
				c.ReadCapacity.Consumed = aws.Float64(mdr.Values[0] / 60)
			}
		case strings.ToLower(DDBReadThrottleEvents):
			if len(mdr.Values) >= 1 {
				// FIXME: looking back 3m means we scale up multiple times if interval is < 3m
				if time.Now().Sub(mdr.Timestamps[0]) < 3*time.Minute {
					c.ReadCapacity.Throttles = mdr.Values[0]
				}
			}
		case strings.ToLower(DDBConsumedWriteCapacityUnits):
			if len(mdr.Values) >= 1 {
				c.WriteCapacity.Consumed = aws.Float64(mdr.Values[0] / 60)
			}
		case strings.ToLower(DDBWriteThrottleEvents):
			if len(mdr.Values) >= 1 {
				// FIXME: looking back 3m means we scale up multiple times if interval is < 3m
				if time.Now().Sub(mdr.Timestamps[0]) < 3*time.Minute {
					c.WriteCapacity.Throttles = mdr.Values[0]
				}
			}
		default:
			return nil, fmt.Errorf("unknown cloudwatch query ID: %s", *mdr.Id)
		}
	}

	return &c, nil
}

func (s *Scaler) getCWMetrics(ctx context.Context, table string, metricName ...string) (*cloudwatch.GetMetricDataOutput, error) {
	var queries []cloudwatchTypes.MetricDataQuery

	for _, mn := range metricName {
		query := cloudwatchTypes.MetricDataQuery{
			Id: aws.String(strings.ToLower(mn)),
			MetricStat: &cloudwatchTypes.MetricStat{
				Metric: &cloudwatchTypes.Metric{
					Dimensions: []cloudwatchTypes.Dimension{
						{
							Name:  aws.String("TableName"),
							Value: &table,
						},
					},
					MetricName: aws.String(mn),
					Namespace:  aws.String(DDBCWNamespace),
				},
				Period: aws.Int32(60),
				Stat:   aws.String("Sum"),
			},
		}
		queries = append(queries, query)
	}

	return s.cwClient.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{
		StartTime:         aws.Time(time.Now().Add(-15 * time.Minute)),
		EndTime:           aws.Time(time.Now()),
		ScanBy:            cloudwatchTypes.ScanByTimestampDescending,
		MetricDataQueries: queries,
	})
}
