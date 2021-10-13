package scaler

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	log "github.com/sirupsen/logrus"
)

type capacityType string

const readCapacity = "read"
const writeCapacity = "write"

type Scaler struct {
	Config    Config
	cwClient  *cloudwatch.Client
	ddbClient *dynamodb.Client
}

func New(cfg Config) *Scaler {
	return &Scaler{
		Config:    cfg,
		cwClient:  cloudwatch.NewFromConfig(cfg.AWSConfig),
		ddbClient: dynamodb.NewFromConfig(cfg.AWSConfig),
	}
}

func (s *Scaler) Run(ctx context.Context) error {
	log.Debugf("config: %v", s.Config.ScalingTargets)

	if err := s.Config.Check(); err != nil {
		return fmt.Errorf("config check failed: %w", err)
	}

	ticker := time.NewTicker(s.Config.Interval)

	s.scaleTargets(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Infof("stopping execution: %s", ctx.Err())

			return nil
		case <-ticker.C:
			s.scaleTargets(ctx)
		}
	}
}

func (s *Scaler) scaleTargets(ctx context.Context) {
	for _, target := range s.Config.ScalingTargets {
		err := s.scaleTarget(ctx, target)
		if err != nil {
			log.WithField("tableName", target.TableName).Warnf("skipping scaling action: %s", err)
		}
	}
}

func (s *Scaler) scaleTarget(ctx context.Context, target *TableScalingConfig) error {
	table, err := s.ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(target.TableName)})
	if err != nil {
		return fmt.Errorf("failed to get table status: %w", err)
	}

	if table.Table.TableStatus != "ACTIVE" {
		return fmt.Errorf("table is currently in status: %s", table.Table.TableStatus)
	}

	tableCap, err := s.getTableCapacity(ctx, target.TableName)
	if err != nil {
		return fmt.Errorf("failed to get table capacity: %w", err)
	}

	tableCap.ReadCapacity.Provisioned = table.Table.ProvisionedThroughput.ReadCapacityUnits
	tableCap.WriteCapacity.Provisioned = table.Table.ProvisionedThroughput.WriteCapacityUnits

	log.WithField("tableName", target.TableName).Debugf("table capacity: %v", tableCap)

	if !tableCap.isSafe() {
		return fmt.Errorf("table capacity is not safe: %+v", *tableCap)
	}

	newReadCap, newWriteCap, err := s.calculateNewCapacity(tableCap, target)
	if err != nil {
		return fmt.Errorf("failed to calculate new capacity: %w", err)
	}

	if (newReadCap >= 1 || newWriteCap >= 1) &&
		(newReadCap != *table.Table.ProvisionedThroughput.ReadCapacityUnits || newWriteCap != *table.Table.ProvisionedThroughput.WriteCapacityUnits) {

		if newReadCap < *table.Table.ProvisionedThroughput.ReadCapacityUnits && newReadCap > 0 {
			if time.Now().Sub(target.lastScaleTime) < target.ScaleDownDelay {
				log.WithField("tableName", target.TableName).Debugf("skipping read scale down, next allowed at %s", target.lastScaleTime.Add(target.ScaleDownDelay).Format(time.RFC3339))
				newReadCap = 0
			}
		}

		if newWriteCap < *table.Table.ProvisionedThroughput.WriteCapacityUnits && newWriteCap > 0 {
			if time.Now().Sub(target.lastScaleTime) < target.ScaleDownDelay {
				log.WithField("tableName", target.TableName).Debugf("skipping write scale down, next allowed at %s", target.lastScaleTime.Add(target.ScaleDownDelay).Format(time.RFC3339))
				newWriteCap = 0
			}
		}

		if newReadCap != 0 && newWriteCap != 0 {
			target.lastScaleTime = time.Now()

			err = s.updateTableCapacity(ctx, table.Table, newReadCap, newWriteCap)
			if err != nil {
				return fmt.Errorf("failed to scale table: %w", err)
			}

			return nil
		}
	}

	log.WithField("tableName", target.TableName).Debug("no scaling necessary")

	return nil
}

func (s *Scaler) calculateNewCapacity(tableCap *tableCapacity, target *TableScalingConfig) (int64, int64, error) {
	var newReadCap, newWriteCap int64

	var err error

	if tableCap.ReadCapacity.Throttles > 0 && tableCap.ReadCapacity.Provisioned != nil {
		newReadCap, err = makeThrottlingScalingDecision(tableCap.ReadCapacity, target)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to make read throttle scaling decision: %w", err)
		}
	}

	if tableCap.WriteCapacity.Throttles > 0 && tableCap.WriteCapacity.Provisioned != nil {
		newWriteCap, err = makeThrottlingScalingDecision(tableCap.WriteCapacity, target)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to make write throttle scaling decision: %w", err)
		}
	}

	if newReadCap == 0 {
		newReadCap, err = makeConsumptionScalingDecision(tableCap.ReadCapacity, target)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to make read consumption scaling decision: %w", err)
		}
	}

	if newWriteCap == 0 {
		newWriteCap, err = makeConsumptionScalingDecision(tableCap.WriteCapacity, target)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to make write consumption scaling decision: %w", err)
		}
	}

	return newReadCap, newWriteCap, nil
}

func (s *Scaler) updateTableCapacity(ctx context.Context, table *dynamodbTypes.TableDescription, newReadCap int64, newWriteCap int64) error {
	provisionThroughput := dynamodbTypes.ProvisionedThroughput{
		ReadCapacityUnits:  table.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits,
	}

	if newReadCap > 0 {
		provisionThroughput.ReadCapacityUnits = aws.Int64(newReadCap)
	}

	if newWriteCap > 0 {
		provisionThroughput.WriteCapacityUnits = aws.Int64(newWriteCap)
	}

	_, err := s.ddbClient.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:             table.TableName,
		ProvisionedThroughput: &provisionThroughput,
	})
	if err != nil {
		return fmt.Errorf("failed to update table: %w", err)
	}

	log.WithField("tableName", *table.TableName).Infof("updated table capacity: read=%d,write=%d", newReadCap, newWriteCap)

	return nil
}

func makeThrottlingScalingDecision(c capacity, target *TableScalingConfig) (int64, error) {
	switch c.CapType {
	case readCapacity:
		result := (c.Throttles + float64(*c.Provisioned)) * (1 + target.ReadBufferCapacity)
		if result > 40000 {
			result = 40000
		}

		if result > target.ReadUpperBound {
			result = target.ReadUpperBound
		}

		return int64(math.Round(result)), nil
	case writeCapacity:
		result := (c.Throttles + float64(*c.Provisioned)) * (1 + target.WriteBufferCapacity)
		if result > 40000 {
			result = 40000
		}

		if result > target.WriteUpperBound {
			result = target.WriteUpperBound
		}

		return int64(math.Round(result)), nil
	default:
		return 0, fmt.Errorf("unknown capacity type: %s", c.CapType)
	}
}

func makeConsumptionScalingDecision(c capacity, target *TableScalingConfig) (int64, error) {
	switch c.CapType {
	case readCapacity:
		return calculateNewBoundary(float64(*c.Provisioned), *c.Consumed, target.ReadBufferCapacity, target.ReadLowerBound, target.ReadUpperBound), nil
	case writeCapacity:
		return calculateNewBoundary(float64(*c.Provisioned), *c.Consumed, target.ReadBufferCapacity, target.ReadLowerBound, target.ReadUpperBound), nil
	default:
		return 0, fmt.Errorf("unknown capacity type: %s", c.CapType)
	}
}

func calculateNewBoundary(provisioned, consumed, buffer, lowerBound, upperBound float64) int64 {
	scaleUpBoundary := provisioned * buffer

	if consumed > scaleUpBoundary {
		newBoundary := consumed * (1 + buffer)
		if newBoundary > upperBound {
			newBoundary = upperBound
		}

		return int64(math.Round(newBoundary))
	}

	newBoundary := consumed * (1 + buffer)

	if newBoundary < lowerBound {
		newBoundary = lowerBound
	}

	return int64(math.Round(newBoundary))
}
