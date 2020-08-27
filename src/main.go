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
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const (
	defaultIterations    uint   = 1
	defaultStreamSize    uint64 = 1024 * 1024 * 128 // 128 M
	defaultAcks          int    = 1
	defaultReplicas      uint   = 3
	defaultTopicMax      uint   = 4
	defaultTopicMin      uint   = 4
	defaultPartitionMax  uint   = 8
	defaultPartitionMin  uint   = 8
	defaultPartitionStep uint   = 2
	defaultMsgMax        uint   = 2 * 1024
	defaultMsgMin        uint   = 128
	defaultMsgStep       uint   = 4 // divisor for calc of next msgSize
	defaultTxRetries     int    = 10
)

// this is used both for configuration
// and results of a streaming test
type streamData struct {
	c             *kafka.Consumer
	p             *kafka.Producer
	evtChan       chan kafka.Event
	txID          uuid.UUID
	wgConsumer    *sync.WaitGroup
	wgProducer    *sync.WaitGroup
	wgProgress    *sync.WaitGroup
	topicName     string
	partition     uint
	msgSize       uint
	msgCount      uint
	progress      uint
	producerStart time.Time
	consumerStop  time.Time
	maxLatency    time.Duration
	minLatency    time.Duration
	totalLatency  time.Duration
}

// Globals
var conf *Config
var configFile string
var sSize uint64
var acks int
var tests, mMax, mMin, mStep, pMax, pMin, pStep, tMax, tMin, replicas uint
var withCompletion, withEoS, delete, reportPartitions, progressBar, debug bool

// HelloKafka is a demo function that belongs in hello-kafka
func main() {

	var e error
	configFile, sSize,
		acks, replicas, tests,
		mMax, mMin, mStep,
		pMax, pMin, pStep,
		tMax, tMin,
		withCompletion, withEoS,
		delete, reportPartitions, progressBar, debug = parseArgs()

	println("Hello Kafka\n")
	dprint("Reading config from: %s\n", configFile)
	conf, e = ReadConfig(configFile)
	if e != nil {
		panic(fmt.Sprintf("Can't read config %s", configFile))
	}
	dprint("Config: %v\n", conf)
	if delete {
		if e := deleteAllTopics(); e != nil {
			panic(e.Error())
		}
	}
	runStreams()
}

func runStreams() {
	for i := tMin; i <= tMax; i++ {
		runTopicsStream(i)
	}
}

func runTopicsStream(topics uint) {
	for i := pMin; i <= pMax; i *= pStep {
		runPartitionStream(topics, i)
	}
}

func runPartitionStream(topics, partitions uint) {
	for i := mMin; i <= mMax; i *= mStep {
		runMsgStream(topics, partitions, i)
	}
}

func runMsgStream(topics, partitions, msgSize uint) {
	var i uint
	for i = 0; i < tests; i++ {
		str := fmt.Sprintf("Iteration %d Stream %d Topics %d Prtns %d MsgSize %d Acks %d Replicas %d Completions %t EoS %t",
			(i + 1), sSize, topics, partitions, msgSize, acks, replicas, withCompletion, withEoS)
		printStr(1, str)
		printStr(len(str), "-")
		testKafka(topics, partitions, msgSize)
	}
}

func testKafka(topics, partitions, msgSize uint) {
	var wgConsumer sync.WaitGroup
	var wgProducer sync.WaitGroup
	var wgProgress sync.WaitGroup
	var testData map[uint]*streamData = make(map[uint]*streamData)

	//ctx := context.Background()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testSetup(ctx, topics, partitions, msgSize, &wgConsumer, &wgProducer, &wgProgress, testData)

	// Start all consumers first
	for i := range testData {
		if progressBar {
			wgProgress.Add(1)
			go updateProgressBar(testData[i])
		}
		wgConsumer.Add(1)
		go consumeStream(testData[i])
	}
	// Now start producers - they will run immediately
	for i := range testData {
		wgProducer.Add(1)
		go produceStream(ctx, testData[i])
	}
	dprint("Waiting for Producers:\n")
	wgProducer.Wait()
	dprint("Producers Completed\n")

	if progressBar {
		dprint("Waiting for Progress bars:\n")
		wgProgress.Wait()
		dprint("Progress bars completed")
	}
	dprint("Waiting for Consumers:\n")
	wgConsumer.Wait()
	dprint("Consumers Completed\n")
	for i, cfg := range testData {
		if i%partitions == 0 {
			// for each topic
			cfg.c.Close()
			cfg.p.Close()
			deleteTopic(cfg.topicName)
		}
	}
	testResult(topics, partitions, msgSize, testData)
}

func testSetup(ctx context.Context, topics, partitions, msgSize uint, wgConsumer, wgProducer, wgProgress *sync.WaitGroup, testData map[uint]*streamData) {
	var pt, t uint
	var c *kafka.Consumer
	var p *kafka.Producer
	var txid uuid.UUID

	postfix := string(uuid.New().String())[0:7] // postfix identifies a test iteration

	for t = 0; t < topics; t++ {
		topic := topicName(topics, t, partitions, msgSize) + "-" + postfix
		dprint("Topic: %s\n", topic)
		createTopic(topic, partitions, replicas)
	}
	// something related to the broker-side request timeout doesn't seem to work right
	time.Sleep(2 * time.Second) // allow new topics to settle

	for t = 0; t < topics; t++ {
		var e error
		topic := topicName(topics, t, partitions, msgSize) + "-" + postfix

		if withEoS {
			txid = uuid.New()
		}
		p, e = launchProducer(topic, partitions, acks, txid)
		if e != nil {
			panic("Cannot launch producer: " + e.Error())
		}
		if withEoS {
			if e = p.InitTransactions(ctx); e != nil {
				panic("Cannot init EoS transactions: " + e.Error())
			}
		}
		c, e = launchConsumer(topic, partitions)
		if e != nil {
			panic("Cannot launch consumer: " + e.Error())
		}

		for pt = 0; pt < partitions; pt++ {
			testCfg := streamData{topicName: topic, c: c, p: p, txID: txid,
				wgConsumer: wgConsumer, wgProducer: wgProducer, wgProgress: wgProgress,
				partition: pt, msgSize: msgSize,
				minLatency: math.MaxInt32}

			if withCompletion || withEoS {
				testCfg.evtChan = make(chan kafka.Event)
			}
			testData[t*partitions+pt] = &testCfg
		}
	}
}

// testResults reports the result of a test Run.
// reporting can be done at the topic level or with partition granularity
// the testData set contains min, max, and total latency for each partition
// as well as total time to deliver all messages on each partition
// and the number of messages sent and received
func testResult(topics, partitions, msgSize uint, testData map[uint]*streamData) {
	var p, t uint
	var result streamData

	// Absolutes across the entire test
	var latencyMax time.Duration
	var latencyMin time.Duration = time.Duration(math.MaxInt64)
	var latencyTotal time.Duration
	var latencyAvg time.Duration
	var timeTotal time.Duration // total consumer stop-producer start
	var timeAvg time.Duration
	var msgTotal uint
	var throughput float64
	var msgPerSec float64
	var summaryFile, topicFile, partitionFile *os.File

	summaryFile = openReportFile("summary", topics, partitions, msgSize)
	defer summaryFile.Close()
	topicFile = openReportFile("topic", topics, partitions, msgSize)
	defer topicFile.Close()
	if reportPartitions {
		partitionFile = openReportFile("partition", topics, partitions, msgSize)
		defer partitionFile.Close()
	}

	println()
	println()
	printHeader()
	for t = 0; t < topics; t++ {
		// Per-topic stats
		var topicLatencyMax time.Duration
		var topicLatencyMin time.Duration = time.Duration(math.MaxInt64)
		var topicLatencyTotal time.Duration
		var topicLatencyAvg time.Duration
		var topicTimeTotal time.Duration // all partitions' consumer stop-producer start
		var topicTimeAvg time.Duration
		var topicMsgTotal uint
		var topicThroughput float64 // Bytes / second
		var topicMsgPerSec float64
		for p = 0; p < partitions; p++ {
			var partitionTime time.Duration
			var partitionLatencyAvg time.Duration
			var partitionThroughput float64
			var partitionMsgPerSec float64

			result = *testData[t*partitions+p]
			if p%partitions != result.partition {
				panic(fmt.Sprint("Partition mismatch: ", p, " : ", result.partition))
			}

			// per-topic
			if topicLatencyMax < result.maxLatency {
				topicLatencyMax = result.maxLatency
			}
			if topicLatencyMin > result.minLatency {
				topicLatencyMin = result.minLatency
			}

			partitionTime = result.consumerStop.Sub(result.producerStart)
			topicTimeTotal += partitionTime
			topicLatencyTotal += result.totalLatency
			topicMsgTotal += result.msgCount

			if reportPartitions {
				partitionLatencyAvg = time.Duration(int64(result.totalLatency) / int64(result.msgCount))
				partitionMsgPerSec = float64(result.msgCount) / partitionTime.Seconds()
				partitionThroughput = float64(sSize) / partitionTime.Seconds() / 1024
				printResultLn(int(t), int(result.partition), result.msgSize, result.msgCount,
					result.minLatency, result.maxLatency, partitionLatencyAvg, partitionTime, partitionThroughput, partitionMsgPerSec)
				printResultCSV(partitionFile, int(t), int(result.partition), result.msgSize, result.msgCount,
					result.minLatency, result.maxLatency, partitionLatencyAvg, partitionTime, partitionThroughput, partitionMsgPerSec)
			}
		}
		if reportPartitions && t < topics {
			printDashes()
		}
		timeTotal += topicTimeTotal
		latencyTotal += topicLatencyTotal
		msgTotal += topicMsgTotal
		// Overall
		if latencyMax < topicLatencyMax {
			latencyMax = topicLatencyMax
		}
		if latencyMin > topicLatencyMin {
			latencyMin = topicLatencyMin
		}
		// once per topic
		topicLatencyAvg = time.Duration(int64(topicLatencyTotal) / int64(topicMsgTotal))
		topicTimeAvg = time.Duration(int64(topicTimeTotal) / int64(partitions))
		topicMsgPerSec = float64(topicMsgTotal) / topicTimeAvg.Seconds()
		topicThroughput = float64(sSize*uint64(partitions)) / topicTimeAvg.Seconds() / 1024

		printResultLn(int(t), int(partitions), result.msgSize, topicMsgTotal,
			topicLatencyMin, topicLatencyMax, topicLatencyAvg, topicTimeTotal, topicThroughput, topicMsgPerSec)
		printResultCSV(topicFile, int(t), int(partitions), result.msgSize, topicMsgTotal,
			topicLatencyMin, topicLatencyMax, topicLatencyAvg, topicTimeTotal, topicThroughput, topicMsgPerSec)
		if reportPartitions && t < topics {
			printDashes()
			println()
		}
	}

	latencyAvg = time.Duration(int64(latencyTotal) / int64(msgTotal))
	timeAvg = time.Duration(int64(timeTotal) / int64(topics*partitions))
	msgPerSec = float64(msgTotal) / timeAvg.Seconds()
	throughput = float64(sSize*uint64(partitions)*uint64(topics)) / timeAvg.Seconds() / 1024

	println("Test Summary:")
	printEquals()
	printHeader()
	printResultLn(int(topics), int(partitions), result.msgSize, msgTotal, latencyMin, latencyMax, latencyAvg,
		timeTotal, throughput, msgPerSec)
	printResultCSV(summaryFile, int(topics), int(partitions), result.msgSize, msgTotal,
		latencyMin, latencyMax, latencyAvg, timeTotal, throughput, msgPerSec)
	printEquals()
}

func getPartition(partitions int, index int) int {
	return index % partitions
}

// parseArgs parses the command line arguments and
// returns the config files and topic on success, or exits on error
func parseArgs() (string, uint64, int,
	uint, uint, uint, uint, uint, uint, uint, uint, uint, uint,
	bool, bool, bool, bool, bool, bool) {

	configFile := flag.String("f", "", "kafka configuration file")

	sSize := flag.Uint64("s", defaultStreamSize, "Bytes to stream per partition")
	acks := flag.Int("a", defaultAcks, "Broker acks")

	tests := flag.Uint("i", defaultIterations, "Test iterations")

	mMax := flag.Uint("M", defaultMsgMax, "Max message size to test")
	mMin := flag.Uint("m", defaultMsgMin, "Min message size to test")
	mStep := flag.Uint("mS", defaultMsgStep, "Message size step factor")

	pMax := flag.Uint("P", defaultPartitionMax, "Max partitions to test")
	pMin := flag.Uint("p", defaultPartitionMin, "Min partitions to test")
	pStep := flag.Uint("pS", defaultPartitionStep, "Partition count step factor")

	replicas := flag.Uint("r", defaultReplicas, "Partition replicas")

	tMax := flag.Uint("T", defaultTopicMax, "Max number of topics to test")
	tMin := flag.Uint("t", defaultTopicMin, "Min number of topics to test")

	completions := flag.Bool("c", false, "Wait for completion (synchronous)")
	eos := flag.Bool("eos", false, "Exactly once semantics")
	delete := flag.Bool("D", false, "Delete topics before test")
	reportPartitions := flag.Bool("pr", false, "Report per-partition results")
	progressBar := flag.Bool("pg", false, "Show progress bar")
	debug := flag.Bool("d", false, "Show debug info")

	flag.Parse()
	if *configFile == "" {
		flag.Usage()
		os.Exit(2) // the same exit code flag.Parse uses
	}
	// allow only the lower case, m p t parameters to be used
	if *mMin > *mMax {
		*mMax = *mMin
	}
	if *pMin > *pMax {
		*pMax = *pMin
	}
	if *tMin > *tMax {
		*tMax = *tMin
	}
	if *eos {
		*completions = true // EoS requires this
		*acks = -1
	}
	return *configFile, *sSize, *acks, *replicas,
		*tests,
		*mMax, *mMin, *mStep,
		*pMax, *pMin, *pStep,
		*tMax, *tMin,
		*completions, *eos,
		*delete, *reportPartitions, *progressBar,
		*debug
}
