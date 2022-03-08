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

package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

func NewKafkaWorker(cg sarama.ConsumerGroup, opts ...Option) *Worker {
	opt := evaluateRunOptions(opts)

	return &Worker{
		cg: cg,

		onSetup:   opt.onSetup,
		onCleanup: opt.onCleanup,

		topicHandler: make(map[string]Handler),
	}
}

// Handler handles kafka message.
//
// To stop consuming, return error except ErrSkipMarkOffset.
// Return ErrSkipMarkOffset to skip mark commit offset.
type Handler func(context.Context, *sarama.ConsumerMessage) error

type Worker struct {
	cg sarama.ConsumerGroup

	onSetup   func(sarama.ConsumerGroupSession) error
	onCleanup func(sarama.ConsumerGroupSession) error

	topicHandler map[string]Handler
	middlewares  []func(Handler) Handler

	cancel context.CancelFunc
}

func (w *Worker) Use(middlewares ...func(Handler) Handler) {
	w.middlewares = append(w.middlewares, middlewares...)
}

func (w *Worker) RegisterHandler(topic string, handler Handler) {
	if _, ok := w.topicHandler[topic]; ok {
		panic(fmt.Errorf("%s topic handler already registered", topic))
	}

	for i := len(w.middlewares) - 1; i >= 0; i-- {
		handler = w.middlewares[i](handler)
	}
	w.topicHandler[topic] = handler
}

func (w *Worker) Start() error {
	return w.StartContext(context.Background())
}

func (w *Worker) StartContext(ctx context.Context) error {
	sctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	topics := w.topics()
	handler := w.consumerGroupHandler()

	for {
		select {
		default:
		case <-ctx.Done():
			return nil
		}

		if err := w.cg.Consume(sctx, topics, handler); err != nil {
			return err
		}
	}
}

func (w *Worker) GracefulStop() {
	w.cancel()
}

func (w *Worker) topics() []string {
	topics := make([]string, 0, len(w.topicHandler))
	for topic := range w.topicHandler {
		topics = append(topics, topic)
	}
	return topics
}

func (w *Worker) consumerGroupHandler() sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{w: w}
}
