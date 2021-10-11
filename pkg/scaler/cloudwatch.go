package scaler

import (
	"context"
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
	readCapacity  capacity
	writeCapacity capacity
}

type capacity struct {
	capType     capacityType
	consumed    *float64
	provisioned *int64
	throttles   float64
}

func (tc *tableCapacity) isSafe() bool {
	if tc.readCapacity.consumed == nil {
		return false
	}

	if tc.readCapacity.provisioned == nil {
		return false
	}

	if tc.writeCapacity.consumed == nil {
		return false
	}

	if tc.writeCapacity.provisioned == nil {
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
		readCapacity: capacity{
			capType: readCapacity,
		},
		writeCapacity: capacity{
			capType: writeCapacity,
		},
	}

	for _, mdr := range metrics.MetricDataResults {
		switch *mdr.Id {
		case strings.ToLower(DDBConsumedReadCapacityUnits):
			if len(mdr.Values) >= 1 {
				c.readCapacity.consumed = aws.Float64(mdr.Values[0] / 60)
			}
		case strings.ToLower(DDBReadThrottleEvents):
			if len(mdr.Values) >= 1 {
				if time.Now().Sub(mdr.Timestamps[0]) < 3*time.Minute {
					c.readCapacity.throttles = mdr.Values[0]
				}
			}
		case strings.ToLower(DDBConsumedWriteCapacityUnits):
			if len(mdr.Values) >= 1 {
				c.writeCapacity.consumed = aws.Float64(mdr.Values[0] / 60)
			}
		case strings.ToLower(DDBWriteThrottleEvents):
			if len(mdr.Values) >= 1 {
				if time.Now().Sub(mdr.Timestamps[0]) < 3*time.Minute {
					c.writeCapacity.throttles = mdr.Values[0]
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
