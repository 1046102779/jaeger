// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	smocks "github.com/Shopify/sarama/mocks"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"
	"go.uber.org/zap"

	kmocks "github.com/jaegertracing/jaeger/cmd/ingester/app/consumer/mocks"
	"github.com/jaegertracing/jaeger/cmd/ingester/app/processor"
	pmocks "github.com/jaegertracing/jaeger/cmd/ingester/app/processor/mocks"
	"github.com/jaegertracing/jaeger/pkg/kafka/consumer"
)

//go:generate mockery -dir ../../../../pkg/kafka/config/ -name Consumer
//go:generate mockery -dir ../../../../../vendor/github.com/bsm/sarama-cluster/ -name PartitionConsumer

const (
	topic     = "morekuzambu"
	partition = int32(316)
	msgOffset = int64(1111110111111)
)

func TestConstructor(t *testing.T) {
	newConsumer, err := New(Params{})
	assert.NoError(t, err)
	assert.NotNil(t, newConsumer)
}

// partitionConsumerWrapper wraps a Sarama partition consumer into a Sarama cluster partition consumer
type partitionConsumerWrapper struct {
	topic     string
	partition int32

	sarama.PartitionConsumer
}

func (s partitionConsumerWrapper) Partition() int32 {
	return s.partition
}

func (s partitionConsumerWrapper) Topic() string {
	return s.topic
}

func newSaramaClusterConsumer(saramaPartitionConsumer sarama.PartitionConsumer) *kmocks.Consumer {
	pcha := make(chan cluster.PartitionConsumer, 1)
	pcha <- &partitionConsumerWrapper{
		topic:             topic,
		partition:         partition,
		PartitionConsumer: saramaPartitionConsumer,
	}
	saramaClusterConsumer := &kmocks.Consumer{}
	saramaClusterConsumer.On("Partitions").Return((<-chan cluster.PartitionConsumer)(pcha))
	saramaClusterConsumer.On("Close").Return(nil)
	saramaClusterConsumer.On("MarkPartitionOffset", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	return saramaClusterConsumer
}

func newConsumer(
	factory metrics.Factory,
	topic string,
	processor processor.SpanProcessor,
	consumer consumer.Consumer) *Consumer {

	logger, _ := zap.NewDevelopment()
	return &Consumer{
		metricsFactory:     factory,
		logger:             logger,
		internalConsumer:   consumer,
		partitionIDToState: make(map[int32]*consumerState),

		processorFactory: ProcessorFactory{
			topic:          topic,
			consumer:       consumer,
			metricsFactory: factory,
			logger:         logger,
			baseProcessor:  processor,
			parallelism:    1,
		},
	}
}

func TestSaramaConsumerWrapper_MarkPartitionOffset(t *testing.T) {
	sc := &kmocks.Consumer{}
	metadata := "meatbag"
	sc.On("MarkPartitionOffset", topic, partition, msgOffset, metadata).Return()
	sc.MarkPartitionOffset(topic, partition, msgOffset, metadata)
	sc.AssertCalled(t, "MarkPartitionOffset", topic, partition, msgOffset, metadata)
}

func TestSaramaConsumerWrapper_start_Messages(t *testing.T) {
	localFactory := metrics.NewLocalFactory(0)

	msg := &sarama.ConsumerMessage{}

	isProcessed := sync.WaitGroup{}
	isProcessed.Add(1)
	mp := &pmocks.SpanProcessor{}
	mp.On("Process", &saramaMessageWrapper{msg}).Return(func(msg processor.Message) error {
		isProcessed.Done()
		return nil
	})

	saramaConsumer := smocks.NewConsumer(t, &sarama.Config{})
	mc := saramaConsumer.ExpectConsumePartition(topic, partition, msgOffset)
	mc.ExpectMessagesDrainedOnClose()

	saramaPartitionConsumer, e := saramaConsumer.ConsumePartition(topic, partition, msgOffset)
	require.NoError(t, e)

	undertest := newConsumer(localFactory, topic, mp, newSaramaClusterConsumer(saramaPartitionConsumer))

	undertest.partitionIDToState = map[int32]*consumerState{
		partition: {
			partitionConsumer: &partitionConsumerWrapper{
				topic:             topic,
				partition:         partition,
				PartitionConsumer: &kmocks.PartitionConsumer{},
			},
		},
	}

	undertest.Start()

	mc.YieldMessage(msg)
	isProcessed.Wait()

	mp.AssertExpectations(t)
	// Ensure that the partition consumer was updated in the map
	assert.Equal(t, saramaPartitionConsumer.HighWaterMarkOffset(),
		undertest.partitionIDToState[partition].partitionConsumer.HighWaterMarkOffset())
	undertest.Close()

	partitionTag := map[string]string{"partition": fmt.Sprint(partition)}
	testutils.AssertCounterMetrics(t, localFactory, testutils.ExpectedMetric{
		Name:  "sarama-consumer.messages",
		Tags:  partitionTag,
		Value: 1,
	})
	testutils.AssertGaugeMetrics(t, localFactory, testutils.ExpectedMetric{
		Name:  "sarama-consumer.current-offset",
		Tags:  partitionTag,
		Value: 1,
	})
	testutils.AssertGaugeMetrics(t, localFactory, testutils.ExpectedMetric{
		Name:  "sarama-consumer.offset-lag",
		Tags:  partitionTag,
		Value: 0,
	})
}

func TestSaramaConsumerWrapper_start_Errors(t *testing.T) {
	localFactory := metrics.NewLocalFactory(0)

	saramaConsumer := smocks.NewConsumer(t, &sarama.Config{})
	mc := saramaConsumer.ExpectConsumePartition(topic, partition, msgOffset)
	mc.ExpectErrorsDrainedOnClose()

	saramaPartitionConsumer, e := saramaConsumer.ConsumePartition(topic, partition, msgOffset)
	require.NoError(t, e)

	undertest := newConsumer(localFactory, topic, &pmocks.SpanProcessor{}, newSaramaClusterConsumer(saramaPartitionConsumer))

	undertest.Start()
	mc.YieldError(errors.New("Daisy, Daisy"))

	for i := 0; i < 1000; i++ {
		time.Sleep(time.Millisecond)

		c, _ := localFactory.Snapshot()
		if len(c) == 0 {
			continue
		}

		partitionTag := map[string]string{"partition": fmt.Sprint(partition)}
		testutils.AssertCounterMetrics(t, localFactory, testutils.ExpectedMetric{
			Name:  "sarama-consumer.errors",
			Tags:  partitionTag,
			Value: 1,
		})
		undertest.Close()
		return
	}

	t.Fail()
}
