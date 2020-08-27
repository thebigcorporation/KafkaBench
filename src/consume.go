package main

/*
Copyright (C) 2020 Manetu Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"fmt"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar"
	"google.golang.org/protobuf/proto"
)

func launchConsumer(topic string, partitions uint) (*kafka.Consumer, error) {

	localCfg := kafka.ConfigMap{
		"group.id":                topic,
		"fetch.wait.max.ms":       100,
		"heartbeat.interval.ms":   3000,
		"session.timeout.ms":      10000,
		"max.poll.interval.ms":    300000,
		"socket.keepalive.enable": "true",
		"auto.offset.reset":       "earliest",
	}
	securityConfig(conf, &localCfg)
	c, e := kafka.NewConsumer(&localCfg)
	if e != nil {
		return nil, errors.Wrapf(e, "kafka: Failed to initialize consumer")
	}
	if e = c.SubscribeTopics([]string{topic}, nil); e != nil {
		return nil, errors.Wrapf(e, "kafka: subscribe [%s]", topic)
	}
	meta, e := c.GetMetadata(&topic, false, 30000)
	if e != nil {
		return nil, errors.Wrapf(e, "kafka: subscribe: meta [%s]", topic)
	}
	nPart, e := pCount(meta, topic)
	if e != nil {
		return nil, errors.Wrapf(e, "kafka: pCount")
	}
	if nPart <= 0 {
		return nil, errors.Errorf("kafka: subscribe: [%s] no partitions", topic)
	}
	if nPart != partitions {
		return nil, errors.Errorf("Consumer partitions: %d Expected: %d", nPart, partitions)
	}
	return c, nil
}

func consumeStream(cfg *streamData) {
	defer cfg.wgConsumer.Done()
	var up bool = true
	var min time.Duration = time.Duration(math.MaxInt64)
	var max time.Duration
	var msgCount uint
	var latencySum time.Duration
	var i uint64

	dprint("Consuming Stream: %s Partition %d\n", cfg.topicName, cfg.partition)
	for i = 0; i < sSize; {
		var flight time.Duration
		if !up {
			break
		}
		dprint("Topic: %s remaining: %d / %d\n", cfg.topicName, sSize-i, sSize)
		cfg.progress = uint(i * 100 / sSize)
		ev := cfg.c.Poll(-1)
		if ev == nil {
			continue
		}
		switch x := ev.(type) {
		case *kafka.Message:
			m := ev.(*kafka.Message)
			dprint("Received message on topic: %s payload size: %d\n", cfg.topicName, len(m.Value))
			flight = processTimeStamp(m)
			msgCount++
			latencySum += flight
			if flight > max {
				max = flight
			}
			if flight < min {
				min = flight
			}
			dprint("Topic: %s Partition: %d MgsSize: %d Min: %d Max: %d\n",
				cfg.topicName, cfg.partition, cfg.msgSize, min, max)
			i += uint64(cfg.msgSize) //len(m.Value)
		case kafka.Error:
			if x.Code() == kafka.ErrAllBrokersDown {
				up = false
				panic(fmt.Sprintf("kafka Error: %s", x.Error()))
			}
			dprint("Kafka error: %s", x.Error())
		case kafka.PartitionEOF:
		default:
		}
	}
	cfg.consumerStop = time.Now()
	cfg.progress = 100
	cfg.totalLatency = latencySum
	cfg.msgCount = msgCount
	cfg.maxLatency = max
	cfg.minLatency = min
}

func updateProgressBar(cfg *streamData) {
	defer cfg.wgProgress.Done()
	var lastprogress, percentChange uint
	bar := progressbar.New(100)
	for cfg.progress < 100 {
		percentChange = cfg.progress - lastprogress
		if percentChange > 0 {
			bar.Add(int(percentChange))
			lastprogress = cfg.progress
		}
		time.Sleep(500 * time.Millisecond)
	}
	percentChange = cfg.progress - lastprogress
	bar.Add(int(percentChange))
}

func processTimeStamp(m *kafka.Message) time.Duration {
	var ts timestamp.Timestamp
	var tTx time.Time
	tRx := time.Now()

	if e := proto.Unmarshal(m.Key, &ts); e != nil {
		panic(fmt.Sprintf("failed to unmarshal timestamp: %s\n", e.Error()))
	}
	tTx, e := ptypes.Timestamp(&ts)
	if e != nil {
		panic(fmt.Sprintf("failed to convert timestamp: %s\n", e.Error()))
	}
	return tRx.Sub(tTx)
}
