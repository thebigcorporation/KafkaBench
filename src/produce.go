
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
	"crypto/rand"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/proto"
)

func launchProducer(topic string, partitions uint, acks int) (*kafka.Producer, error) {

	localCfg := kafka.ConfigMap{
		"max.in.flight.requests.per.connection": 1,
		"retries":                               10,
		"acks":                                  fmt.Sprintf("%d", acks),
		"delivery.timeout.ms":                   60000,
	}
	securityConfig(conf, &localCfg)
	p, e := kafka.NewProducer(&localCfg)
	if e != nil {
		return nil, errors.Wrap(e, "kafka: producer")
	}
	meta, e := p.GetMetadata(&topic, false, 30000)
	if e != nil {
		return nil, e
	}
	nPart, e := pCount(meta, topic)
	if uint(nPart) != partitions {
		return nil, errors.Wrapf(e, "Producer partitions: %d Expected: %d", nPart, partitions)
	}
	tPart := make([]kafka.TopicPartition, nPart)
	for r := range tPart {
		tPart[r].Topic = &topic
		tPart[r].Partition = int32(r)
		tPart[r].Offset = -1
	}
	tResult, e := p.OffsetsForTimes(tPart, 20000)
	for r := range tResult {
		if tResult[r].Offset != 0 {
			return nil, errors.New("Producer partitions: not empty")
		}
	}
	return p, nil
}

func produceStream(cfg *streamData) {
	defer cfg.wgProducer.Done()

	dprint("Producing Stream: " + cfg.topicName + " Partition: " + strconv.Itoa(int(cfg.partition)))
	buf := make([]byte, cfg.msgSize)
	cfg.producerStart = time.Now()
	// sSize is a global constant
	for i := int64(sSize); i > 0; i -= int64(cfg.msgSize) {

		ts, e := ptypes.TimestampProto(time.Now())
		if e != nil {
			panic(fmt.Sprintf("failed to take timestamp: %s\n", e.Error()))
		}
		m, e := proto.Marshal(ts)
		if e != nil {
			panic(fmt.Sprintf("failed to marshal timestamp: %s\n", e.Error()))
		}
		rand.Read(buf)
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &cfg.topicName,
				Partition: int32(cfg.partition)},
			Value: buf,
			Key:   []byte(m),
		}
		if !withCompletion || acks == 0 {
			e = cfg.p.Produce(&msg, nil)
		} else {
			e = cfg.p.Produce(&msg, cfg.evtChan)
		}
		if e != nil {
			panic(fmt.Sprintf("failed to deliver message: %s\n", e.Error()))
		}
		if !withCompletion || acks == 0 {
			continue
		}
		message, e := getCompletion(cfg.evtChan)
		if message.TopicPartition.Error != nil {
			panic(fmt.Sprintf("failed to deliver message: %s\n", message.TopicPartition.Error))
		}
	}
	return
}

func getCompletion(c chan kafka.Event) (*kafka.Message, error) {

	msg := <-c
	m := msg.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m, errors.Wrap(m.TopicPartition.Error, "kafka: produce completion")
	}
	return m, nil
}
