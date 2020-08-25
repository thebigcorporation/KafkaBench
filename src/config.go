
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
	"io/ioutil"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

// SaslConfig properties
type SaslConfig struct {
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// CfgInner properties
type CfgInner struct {
	BootstrapServers []string   `yaml:"bootstrapServers"`
	SecurityProtocol string     `yaml:"securityProtocol"`
	Sasl             SaslConfig `yaml:"sasl"`
}

// Config contains unmarshalled kafka yaml properties
type Config struct {
	Kafka CfgInner `yaml:"kafka"`
}

// ReadConfig reads a config file of this format:
// kafka:
//  bootstrapServers:
//  - kafk:9092
// securityProtocol: SASL_SSL
// sasl:
//   mechanism: PLAIN
//   username: USERNAME
//   password: PASSWORD
// and returns the populated data structure
func ReadConfig(cfgFile string) (*Config, error) {

	b, e := ioutil.ReadFile(cfgFile)
	if e != nil {
		return nil, errors.WithStack(e)
	}
	k := Config{}
	e = yaml.Unmarshal(b, &k)
	if e != nil {
		return nil, errors.WithStack(e)
	}
	//should at least have bootstrap servers
	if len(k.Kafka.BootstrapServers) == 0 {
		return nil, errors.New("kafka: config: bad config")
	}
	return &k, nil
}

func securityConfig(cfg *Config, configMap *kafka.ConfigMap) {
	// The following settings will override any existing ones
	bssList := strings.Join(cfg.Kafka.BootstrapServers, ",")
	configMap.SetKey("bootstrap.servers", bssList)

	switch cfg.Kafka.SecurityProtocol {
	case "SASL_SSL":
		configMap.SetKey("security.protocol", cfg.Kafka.SecurityProtocol)
		configMap.SetKey("sasl.mechanisms", cfg.Kafka.Sasl.Mechanism)
		configMap.SetKey("sasl.username", cfg.Kafka.Sasl.Username)
		configMap.SetKey("sasl.password", cfg.Kafka.Sasl.Password)

	default:
	}
}
