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

package builder

import (
	"fmt"

	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/ingester/app"
	"github.com/jaegertracing/jaeger/cmd/ingester/app/consumer"
	"github.com/jaegertracing/jaeger/cmd/ingester/app/processor"
	kafkaConsumer "github.com/jaegertracing/jaeger/pkg/kafka/consumer"
	"github.com/jaegertracing/jaeger/plugin/storage/kafka"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// CreateConsumer creates a new span consumer for the ingester
func CreateConsumer(logger *zap.Logger, metricsFactory metrics.Factory, spanWriter spanstore.Writer, options app.Options) (*consumer.Consumer, error) {
	var unmarshaller kafka.Unmarshaller
	if options.Encoding == app.EncodingJSON {
		unmarshaller = kafka.NewJSONUnmarshaller()
	} else if options.Encoding == app.EncodingProto {
		unmarshaller = kafka.NewProtobufUnmarshaller()
	} else {
		return nil, fmt.Errorf(`encoding '%s' not recognised, use one of ("%s" or "%s")`,
			options.Encoding, app.EncodingProto, app.EncodingJSON)
	}

	spParams := processor.SpanProcessorParams{
		Writer:       spanWriter,
		Unmarshaller: unmarshaller,
	}
	spanProcessor := processor.NewSpanProcessor(spParams)

	consumerConfig := kafkaConsumer.Configuration{
		Brokers: options.Brokers,
		Topic:   options.Topic,
		GroupID: options.GroupID,
	}
	saramaConsumer, err := consumerConfig.NewConsumer()
	if err != nil {
		return nil, err
	}

	factoryParams := consumer.ProcessorFactoryParams{
		Topic:          options.Topic,
		Parallelism:    options.Parallelism,
		SaramaConsumer: saramaConsumer,
		BaseProcessor:  spanProcessor,
		Logger:         logger,
		Factory:        metricsFactory,
	}
	processorFactory, err := consumer.NewProcessorFactory(factoryParams)
	if err != nil {
		return nil, err
	}

	consumerParams := consumer.Params{
		InternalConsumer: saramaConsumer,
		ProcessorFactory: *processorFactory,
		Factory:          metricsFactory,
		Logger:           logger,
	}
	return consumer.New(consumerParams)
}
