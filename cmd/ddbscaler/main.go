package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/deichindianer/ddb-autoscaler/pkg/scaler"
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

	cfg, err := awsConfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	scalingTargets := []scaler.TableScalingConfiguration{
		{
			TableName:           "test-throttles",
			ReadLowerBound:      1,
			WriteLowerBound:     1,
			WriteUpperBound:     500,
			WriteBufferCapacity: 0.1,
		},
	}

	s := scaler.New(cfg, scalingTargets)
	s.Run(ctx)
}
