
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
	"os"
	"time"
)

func printResultLn(topic, partition int, msgSize, msgCount uint, min, max, avg, runtime time.Duration, tput, msgsec float64) {
	fmt.Printf("  %3d  %3d   %6d  %6d  %6d  %6d  %6d  %7.1f  %7.1f  %7.1fs\n",
		topic, partition, msgSize, msgCount, min.Milliseconds(), max.Milliseconds(), avg.Milliseconds(),
		tput, msgsec, runtime.Seconds())
}

func printResultCSV(file *os.File, topic, partition int, msgSize, msgCount uint, min, max, avg, runtime time.Duration, tput, msgsec float64) {
	reportstr := fmt.Sprintf("%3d, %3d, %6d, %6d, %6d, %6d, %6d, %7.1f, %7.1f, %7.1f\n",
		topic, partition, msgSize, msgCount, min.Milliseconds(), max.Milliseconds(), avg.Milliseconds(),
		tput, msgsec, runtime.Seconds())
	file.WriteString(reportstr)
}

func printHeader() {
	println(" Topic Prtn MsgSize Messages MinLat  MaxLat  AvgLat     Kb/s    Msg/s   Runtime")
	printDashes()
}
func printDashes() {
	println("----------------------------------------------------------------------------------")
}

func printEquals() {
	println("==================================================================================")
}

func writeHeader(file *os.File) {
	file.WriteString("Topic, Prtn, MsgSize, Messages, MinLat, MaxLat, AvgLat, Kbps, Msgps, Runtime\n")
}

func openReportFile(ftype string, topics, partitions, msgSize uint) *os.File {
	switch ftype {
	case "partition":
		if !reportPartitions {
			return nil
		}
	case "topic":
	case "summary":
	default:
		return nil
	}
	bootstrap := conf.Kafka.BootstrapServers[0][0:7]
	filename := fmt.Sprintf("KafkaPerf-%s-%s-T%02d-P%02d-R%02d-M%06db-S%09d-A%02d-C%t.csv",
		bootstrap, ftype, topics, partitions, replicas,
		msgSize, sSize, acks, withCompletion)
	_, fs := os.Stat(filename)
	f, e := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		panic("can't open output file: " + filename)
	}
	if os.IsNotExist(fs) {
		writeHeader(f)
	}
	return f
}
