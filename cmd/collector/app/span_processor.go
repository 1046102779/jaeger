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

package app

import (
	"time"

	"github.com/uber/tchannel-go"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/collector/app/sanitizer"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/queue"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// spanProcessor用于接收model.Span，并作为生产者发送这个数据到channel队列上, 供存储server消费
type spanProcessor struct {
	queue           *queue.BoundedQueue
	metrics         *SpanProcessorMetrics
	preProcessSpans ProcessSpans
	filterSpan      FilterSpan             // filter is called before the sanitizer but after preProcessSpans
	sanitizer       sanitizer.SanitizeSpan // sanitizer is called before processSpan
	processSpan     ProcessSpan
	logger          *zap.Logger
	spanWriter      spanstore.Writer
	reportBusy      bool
	numWorkers      int
}

// channel队列上的数据元素
type queueItem struct {
	queuedTime time.Time
	span       *model.Span
}

// NewSpanProcessor方法创建一个SpanProcessor，并提供5个链式处理，类似于插件形式
// 包括：preProcesses预处理，filters过滤，queues，sanitizes和process models
func NewSpanProcessor(
	spanWriter spanstore.Writer,
	opts ...Option,
) SpanProcessor {
	// 获取SpanProcessor实例, 其中spanWriter用于存储model.Span的io.Writer流，写入到存储
	sp := newSpanProcessor(spanWriter, opts...)

	// 启动numWorkers接收channel队列上的*model.Span
	sp.queue.StartConsumers(sp.numWorkers, func(item interface{}) {
		value := item.(*queueItem)
		// 处理*models.Span，调用SpanProcessor的saveSpan方法存储span,
		sp.processItemFromQueue(value)
	})

	sp.queue.StartLengthReporting(1*time.Second, sp.metrics.QueueLength)

	return sp
}

// spanstore.Writer是获得的io.Writer写入流，这个是直接对接的collector server后端存储
func newSpanProcessor(spanWriter spanstore.Writer, opts ...Option) *spanProcessor {
	options := Options.apply(opts...)
	handlerMetrics := NewSpanProcessorMetrics(
		options.serviceMetrics,
		options.hostMetrics,
		options.extraFormatTypes)
	droppedItemHandler := func(item interface{}) {
		handlerMetrics.SpansDropped.Inc(1)
	}
	boundedQueue := queue.NewBoundedQueue(options.queueSize, droppedItemHandler)

	sp := spanProcessor{
		queue:           boundedQueue,
		metrics:         handlerMetrics,
		logger:          options.logger,
		preProcessSpans: options.preProcessSpans,
		filterSpan:      options.spanFilter,
		sanitizer:       options.sanitizer,
		reportBusy:      options.reportBusy,
		numWorkers:      options.numWorkers,
		spanWriter:      spanWriter,
	}
	// processSpan为处理函数链式, 先预处理，然后再存储span
	sp.processSpan = ChainedProcessSpan(
		options.preSave,
		sp.saveSpan,
	)

	return &sp
}

// Stop方法终止并关闭channel队列
func (sp *spanProcessor) Stop() {
	sp.queue.Stop()
}

// saveSpan方法用于存储*model.Span
func (sp *spanProcessor) saveSpan(span *model.Span) {
	// 直接使用后端存储的io.Writer写入流，写入span
	startTime := time.Now()
	if err := sp.spanWriter.WriteSpan(span); err != nil {
		sp.logger.Error("Failed to save span", zap.Error(err))
	} else {
		sp.logger.Debug("Span written to the storage by the collector",
			zap.Stringer("trace-id", span.TraceID), zap.Stringer("span-id", span.SpanID))
		sp.metrics.SavedBySvc.ReportServiceNameForSpan(span)
	}
	sp.metrics.SaveLatency.Record(time.Now().Sub(startTime))
}

// ProcessSpans方法用于批量的*model.Span，支持两种格式：zipkin, jaeger
func (sp *spanProcessor) ProcessSpans(mSpans []*model.Span, spanFormat string) ([]bool, error) {
	// 进入channel队列之前的预先批处理
	sp.preProcessSpans(mSpans)
	sp.metrics.BatchSize.Update(int64(len(mSpans)))
	retMe := make([]bool, len(mSpans))
	// 入队列，并等待返回，因为如果channel队列满了，则直接返回队列满，错误码：server busy
	for i, mSpan := range mSpans {
		ok := sp.enqueueSpan(mSpan, spanFormat)
		if !ok && sp.reportBusy {
			return nil, tchannel.ErrServerBusy
		}
		retMe[i] = ok
	}
	return retMe, nil
}

// processItemFromQueue方法用于在存储*model.Span之前的预处理, 并存储span
func (sp *spanProcessor) processItemFromQueue(item *queueItem) {
	// processSpan是一个闭包函数，有两个方法构成：1. preSpan 2. saveSpan
	// 它是有一个链式函数构成
	sp.processSpan(sp.sanitizer(item.span))
	sp.metrics.InQueueLatency.Record(time.Now().Sub(item.queuedTime))
}

// enqueueSpan方法用于*model.Span入队列，它作为生产者
func (sp *spanProcessor) enqueueSpan(span *model.Span, originalFormat string) bool {
	spanCounts := sp.metrics.GetCountsForFormat(originalFormat)
	spanCounts.ReceivedBySvc.ReportServiceNameForSpan(span)

	if !sp.filterSpan(span) {
		spanCounts.RejectedBySvc.ReportServiceNameForSpan(span)
		return true // as in "not dropped", because it's actively rejected
	}
	item := &queueItem{
		queuedTime: time.Now(),
		span:       span,
	}
	// 如果队列满了，则返回入队失败
	addedToQueue := sp.queue.Produce(item)
	if !addedToQueue {
		sp.metrics.ErrorBusy.Inc(1)
	}
	return addedToQueue
}
