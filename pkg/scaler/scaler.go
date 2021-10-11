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
	cwClient       *cloudwatch.Client
	ddbClient      *dynamodb.Client
	ScalingTargets []TableScalingConfiguration
}

type TableScalingConfiguration struct {
	TableName           string
	ReadUpperBound      float64
	WriteUpperBound     float64
	ReadLowerBound      float64
	WriteLowerBound     float64
	ReadBufferCapacity  float64
	WriteBufferCapacity float64
}

func New(cfg aws.Config, scalingTargets []TableScalingConfiguration) *Scaler {
	return &Scaler{
		cwClient:       cloudwatch.NewFromConfig(cfg),
		ddbClient:      dynamodb.NewFromConfig(cfg),
		ScalingTargets: scalingTargets,
	}
}

func (s *Scaler) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)

	s.scaleTargets(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Infof("stopping execution: %s", ctx.Err())
			return
		case <-ticker.C:
			s.scaleTargets(ctx)
		}
	}
}

func (s *Scaler) scaleTargets(ctx context.Context) {
	for _, target := range s.ScalingTargets {
		table, err := s.ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(target.TableName)})
		if err != nil {
			log.Warnf("skipping scaling action: failed to get table status for %s: %s", target.TableName, err)
			continue
		}

		if table.Table.TableStatus != "ACTIVE" {
			log.Warnf("skipping scaling action: table %s is currently in status: %s", target.TableName, table.Table.TableStatus)
			continue
		}

		tableCap, err := s.getTableCapacity(ctx, target.TableName)
		if err != nil {
			log.Warnf("failed to get table capacity for %s: %s", target.TableName, err)
		}

		tableCap.readCapacity.provisioned = table.Table.ProvisionedThroughput.ReadCapacityUnits
		tableCap.writeCapacity.provisioned = table.Table.ProvisionedThroughput.WriteCapacityUnits

		if !tableCap.isSafe() {
			log.Errorf("table capacity is not safe, skipping table %s: %+v", target.TableName, *tableCap)
			continue
		}

		var newReadCap, newWriteCap int64

		if tableCap.readCapacity.throttles > 0 && tableCap.readCapacity.provisioned != nil {
			newReadCap, err = makeThrottlingScalingDecision(tableCap.readCapacity, target)
			if err != nil {
				log.Warnf("skipping scaling action: failed to make read throttle scaling decision for %s: %s", target.TableName, err)
				continue
			}
		}

		if tableCap.writeCapacity.throttles > 0 && tableCap.writeCapacity.provisioned != nil {
			newWriteCap, err = makeThrottlingScalingDecision(tableCap.writeCapacity, target)
			if err != nil {
				log.Warnf("skipping scaling action: failed to make write throttle scaling decision for %s: %s", target.TableName, err)
				continue
			}
		}

		if newReadCap == 0 {
			newReadCap, err = makeLowConsumptionScalingDecision(tableCap.readCapacity, target)
			if err != nil {
				log.Warnf("skipping scaling action: failed to make read low consumption scaling decision for %s: %s", target.TableName, err)
				continue
			}
		}

		if newWriteCap == 0 {
			newWriteCap, err = makeLowConsumptionScalingDecision(tableCap.writeCapacity, target)
			if err != nil {
				log.Warnf("skipping scaling action: failed to make write low consumption scaling decision for %s: %s", target.TableName, err)
				continue
			}
		}

		if (newReadCap >= 1 || newWriteCap >= 1) &&
			(newReadCap != *table.Table.ProvisionedThroughput.ReadCapacityUnits || newWriteCap != *table.Table.ProvisionedThroughput.WriteCapacityUnits) {
			provisionThroughput := dynamodbTypes.ProvisionedThroughput{
				ReadCapacityUnits:  table.Table.ProvisionedThroughput.ReadCapacityUnits,
				WriteCapacityUnits: table.Table.ProvisionedThroughput.WriteCapacityUnits,
			}

			if newReadCap > 0 {
				provisionThroughput.ReadCapacityUnits = aws.Int64(newReadCap)
			}

			if newWriteCap > 0 {
				provisionThroughput.WriteCapacityUnits = aws.Int64(newWriteCap)
			}

			_, err = s.ddbClient.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName:             aws.String(target.TableName),
				ProvisionedThroughput: &provisionThroughput,
			})
			if err != nil {
				log.Errorf("failed to scale table %s: %s", target.TableName, err)
			}

			log.Infof("scaled %s to read=%d,write=%d", target.TableName, newReadCap, newWriteCap)
			continue
		}
		log.Debugf("no scaling necessary for %s", target.TableName)
	}
}

func makeThrottlingScalingDecision(c capacity, target TableScalingConfiguration) (int64, error) {
	switch c.capType {
	case readCapacity:
		result := (c.throttles + float64(*c.provisioned)) * (1 + target.ReadBufferCapacity)
		if result > 40000 {
			result = 40000
		}

		if result > target.ReadUpperBound {
			result = target.ReadUpperBound
		}

		return int64(math.Round(result)), nil
	case writeCapacity:
		result := (c.throttles + float64(*c.provisioned)) * (1 + target.WriteBufferCapacity)
		if result > 40000 {
			result = 40000
		}

		if result > target.WriteUpperBound {
			result = target.WriteUpperBound
		}

		return int64(math.Round(result)), nil
	default:
		return 0, fmt.Errorf("unknown capacity type: %s", c.capType)
	}
}

func makeLowConsumptionScalingDecision(c capacity, target TableScalingConfiguration) (int64, error) {
	switch c.capType {
	case readCapacity:
		bufferedTargetCap := *c.consumed * (1 + target.ReadBufferCapacity)

		if bufferedTargetCap < target.ReadLowerBound {
			return int64(target.ReadLowerBound), nil
		}

		if bufferedTargetCap < float64(*c.provisioned) {
			return int64(math.Round(bufferedTargetCap)), nil
		}

		return *c.provisioned, nil
	case writeCapacity:
		bufferedTargetCap := *c.consumed * (1 + target.ReadBufferCapacity)

		if bufferedTargetCap < target.WriteLowerBound {
			return int64(target.WriteLowerBound), nil
		}

		if bufferedTargetCap < float64(*c.provisioned) {
			return int64(math.Round(bufferedTargetCap)), nil
		}

		return *c.provisioned, nil
	default:
		return 0, fmt.Errorf("unknown capacity type: %s", c.capType)
	}
}
