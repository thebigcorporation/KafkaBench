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
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/pkg/errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/proto"
)

func launchProducer(topic string, partitions uint, acks int, txid uuid.UUID) (*kafka.Producer, error) {

	localCfg := kafka.ConfigMap{
		"acks":                                  fmt.Sprintf("%d", acks),
		"delivery.timeout.ms":                   60000,
		"max.in.flight.requests.per.connection": 1,
		"message.send.max.retries":              "10",
		"message.timeout.ms":                    "30000",
		"retries":                               10,
	}
	if !withCompletion {
		localCfg.SetKey("go.delivery.reports", false)
	}
	if withEoS {
		localCfg.SetKey("transactional.id", txid.String())
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
	if e != nil {
		return nil, errors.Wrap(e, "kafka: producer OffsetsForTimes")
	}
	for r := range tResult {
		if tResult[r].Offset != 0 {
			return nil, errors.New("Producer partitions: not empty")
		}
	}
	return p, nil
}

func produceStream(ctx context.Context, cfg *streamData) {
	defer cfg.wgProducer.Done()

	dprint("Producing Stream: %s Partition: %d\n", cfg.topicName, cfg.partition)
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
		// we rely on config parsing to ensure withEoS and withCompletion
		// are never both set, but just in case, let's process EoS first
		switch {
		case withEoS:
			e = writeTransaction(ctx, cfg, &msg)
		case withCompletion:
			e = cfg.p.Produce(&msg, cfg.evtChan)
		default:
			e = cfg.p.Produce(&msg, nil)
		}
		if e != nil {
			panic(fmt.Sprintf("failed to deliver message: %s\n", e.Error()))
		}
		if withCompletion && !withEoS {
			message, e := getCompletion(cfg.evtChan)
			if e != nil {
				panic(fmt.Sprintf("failed to deliver message: %s\n", e.Error()))
			}
			_ = message
		}
	}
	if withCompletion {
		close(cfg.evtChan)
	}
}

func produceMessage() {

}

func getCompletion(c chan kafka.Event) (*kafka.Message, error) {
	msg := <-c
	m := msg.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m, errors.Wrap(m.TopicPartition.Error, "kafka: produce completion")
	}
	return m, nil
}

// Output to Audit log. Expects mAudit.TopicPartition.Topic to be set
func writeTransaction(ctx context.Context, cfg *streamData, msg *kafka.Message) error {
	var e error
	var txAttempts int
	var commitAttempts int

redo:
	txAttempts++
	dprint("Partition %d Tx Attempts: %d\n", msg.TopicPartition.Partition, txAttempts)
	if txAttempts > defaultTxRetries {
		return errors.New("too many retries")
	}
	if e = cfg.p.BeginTransaction(); e != nil {
		cfg.p.AbortTransaction(ctx)
		fmt.Printf("Partition %d tx begin: %s", msg.TopicPartition.Partition, e.Error())
		goto redo
	}
	e = cfg.p.Produce(msg, cfg.evtChan)
	if e != nil {
		if e.(kafka.Error).IsRetriable() {
			cfg.p.AbortTransaction(ctx)
			fmt.Printf("produce tx: %s", e.Error())
			goto redo
		}
		return e
	}
	completion, e := getCompletion(cfg.evtChan)
	if e != nil {
		panic(fmt.Sprintf("failed to deliver message: %s\n", e.Error()))
	}
	if completion != nil && completion.TopicPartition.Error != nil {
		cfg.p.AbortTransaction(ctx)
		fmt.Printf("TopicPartition.Error: %s", completion.TopicPartition.Error.Error())
		goto redo
	}
retry:
	commitAttempts++
	dprint("Commit Attempts: %d\n", commitAttempts)
	if e = cfg.p.CommitTransaction(ctx); e != nil {
		switch {
		case e.(kafka.Error).TxnRequiresAbort():
			cfg.p.AbortTransaction(ctx)
			fmt.Printf("tx commit: %s", e.Error())
			goto redo
		case e.(kafka.Error).IsRetriable():
			if commitAttempts > defaultTxRetries {
				return errors.New(e.(kafka.Error).String())
			}
			fmt.Printf("tx commit: retry %d", commitAttempts)
			goto retry
		default:
			fmt.Printf("tx commit fatal: %s", e.Error())
			return e
		}
	}
	dprint("Committed: %s:%d\n", cfg.topicName, cfg.partition)
	return nil
}
