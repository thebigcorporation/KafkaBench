
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
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

func getAdminClient() (*kafka.AdminClient, error) {

	localCfg := kafka.ConfigMap{
		"broker.version.fallback":     "0.10.0.0",
		"api.version.fallback.ms":     0,
		"metadata.request.timeout.ms": 30000,
	}
	securityConfig(conf, &localCfg)
	aClient, e := kafka.NewAdminClient(&localCfg)
	if e != nil {
		return nil, errors.Wrap(e, "kafka: admin client")
	}
	return aClient, nil
}

func createTopic(topic string, partitions, replicas uint) error {

	tCfg := make(map[string]string)

	tSpec := kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     int(partitions),
		ReplicationFactor: int(replicas),
		Config:            tCfg,
	}
	timeout := kafka.SetAdminOperationTimeout(time.Duration(30000 * time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// wait duration for the operation to finish (or at most 60s)
	aClient, e := getAdminClient()
	if e != nil {
		return errors.Wrapf(e, "kafka: create topic %s", topic)
	}
	results, e := aClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{tSpec},
		timeout)
	if e != nil {
		return errors.Wrapf(e, "kafka: create topic: %s", topic)
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			panic(r.String())
		}
	}
	return nil
}

func deleteTopic(topic string) error {

	topiclist := []string{topic}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	aClient, e := getAdminClient()
	if e != nil {
		return errors.Wrapf(e, "kafka: delete topic %s", topic)
	}
	deltopics, e := aClient.DeleteTopics(ctx, topiclist, nil)
	if e != nil {
		return errors.Wrapf(e, "Kafka: delete topic: %v", deltopics)
	}
	return nil
}

func deleteAllTopics() error {
	aClient, e := getAdminClient()
	if e != nil {
		return errors.Wrap(e, "kafka: delete all topics")
	}
	tMeta, e := aClient.GetMetadata(nil, true, 20000)
	if e != nil {
		return errors.Wrap(e, "Failed to get meta data")
	}
	for topic := range tMeta.Topics {
		if !strings.HasPrefix(topic, "Test-Perf-Topic") {
			continue
		}
		e = deleteTopic(topic)
		if e != nil {
			return errors.Wrap(e, "delete all topics")
		}
		println("Deleted topic: ", topic)
	}
	return nil
}

func topicName(topics, tcount, partitions, msgSize uint) string {
	return fmt.Sprintf("Test-Perf-Topic-%02d-%02d-Partitions-%02d-%db", topics, tcount,
		partitions, msgSize)
}

// pCount returns the number of partitions in the topic
func pCount(m *kafka.Metadata, topic string) (uint, error) {

	metaT, v := m.Topics[topic]
	if !v {
		return 0, errors.Errorf("kafka: pCount: topic not found: %+v", topic)
	}
	pCount := uint(len(metaT.Partitions))
	return pCount, nil
}
