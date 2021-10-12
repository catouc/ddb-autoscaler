package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/deichindianer/ddb-autoscaler/pkg/scaler"
	log "github.com/sirupsen/logrus"
)

var (
	logLevel        = flag.Int("logLevel", 4, "log level (0-6)")
	logReportCaller = flag.Bool("logReportCaller", false, "add caller to log output")
	logFormatJSON   = flag.Bool("logFormatJson", false, "log in json format")
)

func init() {
	flag.Parse()
	log.SetLevel(log.Level(*logLevel))
	log.SetReportCaller(*logReportCaller)
	log.SetFormatter(&log.TextFormatter{})
	if *logFormatJSON {
		log.SetFormatter(&log.JSONFormatter{})
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		cancel()
	}()

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	scalerCfg := scaler.Config{
		AWSConfig: awsCfg,
		Interval:  60 * time.Second,
		ScalingTargets: []*scaler.TableScalingConfig{
			{
				TableName:           "test-throttles",
				ScaleDownDelay:      60 * time.Minute,
				ReadLowerBound:      1,
				WriteLowerBound:     1,
				ReadUpperBound:      500,
				WriteUpperBound:     500,
				ReadBufferCapacity:  0.5,
				WriteBufferCapacity: 0.5,
			},
		},
	}

	s := scaler.New(scalerCfg)

	if err := s.Run(ctx); err != nil {
		log.Error(err.Error())
	}
}
