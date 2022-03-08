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
	"errors"

	"github.com/Shopify/sarama"
)

type consumerGroupHandler struct {
	w *Worker
}

func (cgh *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	if cgh.w.onSetup == nil {
		return nil
	}

	if err := cgh.w.onSetup(session); err != nil {
		return err
	}

	return nil
}

func (cgh *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	if cgh.w.onCleanup == nil {
		return nil
	}

	if err := cgh.w.onCleanup(session); err != nil {
		return err
	}

	return nil
}

func (cgh *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	handler := cgh.w.topicHandler[claim.Topic()]

	for msg := range claim.Messages() {
		if err := handler(context.Background(), msg); err != nil {
			if errors.Is(err, ErrSkipMarkOffset) {
				continue
			}
			return err
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
