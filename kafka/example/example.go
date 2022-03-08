// Copyright 2022 Il Sub Bang
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"

	"github.com/isbang/worker/kafka"
)

var brokerList = []string{"localhost:9092"}
var groupId = "example-kafka-consumer-group"

func main() {
	scfg := sarama.NewConfig()
	scfg.Consumer.Return.Errors = false
	scfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	scfg.Consumer.Offsets.AutoCommit.Enable = true
	scfg.Consumer.Offsets.AutoCommit.Interval = time.Second

	cg, err := sarama.NewConsumerGroup(brokerList, groupId, scfg)
	if err != nil {
		log.Fatalln("fail to make consumer group", err)
	}
	defer cg.Close()

	worker := kafka.NewKafkaWorker(cg)

	worker.Use(func(handler kafka.Handler) kafka.Handler {
		return func(ctx context.Context, message *sarama.ConsumerMessage) error {
			defer func() {
				if v := recover(); v != nil {
					log.Println("!!!panic recovered!!!", v)
				}
			}()
			return handler(ctx, message)
		}
	})

	worker.RegisterHandler("sample-topic", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		fmt.Println(string(message.Value))
		return nil
	})

	worker.RegisterHandler("no-offset-marking-topic", func(ctx context.Context, message *sarama.ConsumerMessage) error {
		return kafka.ErrSkipMarkOffset
	})

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	go func() {
		<-ch
		signal.Stop(ch)

		worker.GracefulStop()
	}()

	if err := worker.Start(); err != nil {
		log.Fatalln("fail to start kafka worker", err)
	}
}
