// Copyright (c) 2017 Uber Technologies, Inc.
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
	"errors"
	"os"

	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	basicB "github.com/jaegertracing/jaeger/cmd/builder"
	"github.com/jaegertracing/jaeger/cmd/collector/app"
	zs "github.com/jaegertracing/jaeger/cmd/collector/app/sanitizer/zipkin"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	errMissingCassandraConfig     = errors.New("Cassandra not configured")
	errMissingMemoryStore         = errors.New("MemoryStore is not provided")
	errMissingElasticSearchConfig = errors.New("ElasticSearch not configured")
)

// SpanHandlerBuilder用于存储一些Collector server连接参数。zipkin和jaeger提交span的handler等
type SpanHandlerBuilder struct {
	logger         *zap.Logger
	metricsFactory metrics.Factory
	collectorOpts  *CollectorOptions
	spanWriter     spanstore.Writer
}

// NewSpanHandlerBuilder返回一个SpanHandlerBuilder实例
func NewSpanHandlerBuilder(cOpts *CollectorOptions, spanWriter spanstore.Writer, opts ...basicB.Option) (*SpanHandlerBuilder, error) {
	options := basicB.ApplyOptions(opts...)

	spanHb := &SpanHandlerBuilder{
		collectorOpts:  cOpts,
		logger:         options.Logger,
		metricsFactory: options.MetricsFactory,
		spanWriter:     spanWriter,
	}

	return spanHb, nil
}

// BuildHandlers构建span handlers(Zipkin, Jaeger)
func (spanHb *SpanHandlerBuilder) BuildHandlers() (app.ZipkinSpansHandler, app.JaegerBatchesHandler) {
	hostname, _ := os.Hostname()
	hostMetrics := spanHb.metricsFactory.Namespace("", map[string]string{"host": hostname})

	zSanitizer := zs.NewChainedSanitizer(
		zs.NewSpanDurationSanitizer(),
		zs.NewSpanStartTimeSanitizer(),
		zs.NewParentIDSanitizer(),
		zs.NewErrorTagSanitizer(),
	)

	// 创建一个SpanProcessor实例，用于处理*model.Span并生产和消费它，最终存储到后端存储中
	spanProcessor := app.NewSpanProcessor(
		spanHb.spanWriter,
		app.Options.ServiceMetrics(spanHb.metricsFactory),
		app.Options.HostMetrics(hostMetrics),
		app.Options.Logger(spanHb.logger),
		app.Options.SpanFilter(defaultSpanFilter),
		app.Options.NumWorkers(spanHb.collectorOpts.NumWorkers),
		app.Options.QueueSize(spanHb.collectorOpts.QueueSize),
	)

	// 分别获取zipkin数据格式的*model.Span处理函数和jaeger数据格式的*model.Span处理函数
	return app.NewZipkinSpanHandler(spanHb.logger, spanProcessor, zSanitizer),
		app.NewJaegerSpanHandler(spanHb.logger, spanProcessor)
}

func defaultSpanFilter(*model.Span) bool {
	return true
}
