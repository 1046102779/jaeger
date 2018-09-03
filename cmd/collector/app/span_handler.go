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
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	zipkinS "github.com/jaegertracing/jaeger/cmd/collector/app/sanitizer/zipkin"
	"github.com/jaegertracing/jaeger/model"
	jConv "github.com/jaegertracing/jaeger/model/converter/thrift/jaeger"
	"github.com/jaegertracing/jaeger/model/converter/thrift/zipkin"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
)

const (
	// JaegerFormatType is for Jaeger Spans
	JaegerFormatType = "jaeger"
	// ZipkinFormatType is for zipkin Spans
	ZipkinFormatType = "zipkin"
	// UnknownFormatType is for spans that do not have a widely defined/well-known format type
	UnknownFormatType = "unknown"
)

// ZipkinSpansHandler interface与JaegerBatchedHandler interface
// 用于接收到的trace包，并存储trace

// ZipkinSpansHandler interface用于接收和存储zipkin spans
type ZipkinSpansHandler interface {
	// SubmitZipkinBatch records a batch of spans in Zipkin Thrift format
	SubmitZipkinBatch(ctx thrift.Context, spans []*zipkincore.Span) ([]*zipkincore.Response, error)
}

// JaegerBatchesHandler interface用于接收和处理jaeger batches
type JaegerBatchesHandler interface {
	// SubmitBatches records a batch of spans in Jaeger Thrift format
	SubmitBatches(ctx thrift.Context, batches []*jaeger.Batch) ([]*jaeger.BatchSubmitResponse, error)
}

// SpanProcessor interface用于处理model spans, 主要是spans入channel队列，等待消费者消费并通过saveSpans方法存储model.Span
type SpanProcessor interface {
	// ProcessSpans processes model spans and return with either a list of true/false success or an error
	ProcessSpans(mSpans []*model.Span, spanFormat string) ([]bool, error)
}

// jaegerBatchesHandler实现了JaegerBatchesHandler interface
type jaegerBatchesHandler struct {
	logger         *zap.Logger
	modelProcessor SpanProcessor
}

// NewJaegerSpanHandler方法用于创建一个JaegerBatchedHandler实例
func NewJaegerSpanHandler(logger *zap.Logger, modelProcessor SpanProcessor) JaegerBatchesHandler {
	return &jaegerBatchesHandler{
		logger:         logger,
		modelProcessor: modelProcessor,
	}
}

// SubmitBatches方法用于接收批量的trace包，并把数据转换为model.Span，并交给span processer处理后发送到channel队列，供消费者获取并存储trace
func (jbh *jaegerBatchesHandler) SubmitBatches(ctx thrift.Context, batches []*jaeger.Batch) ([]*jaeger.BatchSubmitResponse, error) {
	responses := make([]*jaeger.BatchSubmitResponse, 0, len(batches))
	// 遍历trace包
	for _, batch := range batches {
		mSpans := make([]*model.Span, 0, len(batch.Spans))
		// 对于trace包，构建成model.Span
		for _, span := range batch.Spans {
			mSpan := jConv.ToDomainSpan(span, batch.Process)
			mSpans = append(mSpans, mSpan)
		}
		// model.Span数据发送给span processor统一处理，格式包括jaeger和zipkin
		oks, err := jbh.modelProcessor.ProcessSpans(mSpans, JaegerFormatType)
		if err != nil {
			jbh.logger.Error("Collector failed to process span batch", zap.Error(err))
			return nil, err
		}
		batchOk := true
		for _, ok := range oks {
			if !ok {
				batchOk = false
				break
			}
		}

		jbh.logger.Debug("Span batch processed by the collector.", zap.Bool("ok", batchOk))
		res := &jaeger.BatchSubmitResponse{
			Ok: batchOk,
		}
		responses = append(responses, res)
	}
	return responses, nil
}

// zipkinSpanHandler用于zipkin格式的trace包处理和存储
type zipkinSpanHandler struct {
	logger         *zap.Logger
	sanitizer      zipkinS.Sanitizer
	modelProcessor SpanProcessor
}

// NewZipkinSpanHandler创建一个ZipkinSpansHandler实例
func NewZipkinSpanHandler(logger *zap.Logger, modelHandler SpanProcessor, sanitizer zipkinS.Sanitizer) ZipkinSpansHandler {
	return &zipkinSpanHandler{
		logger:         logger,
		modelProcessor: modelHandler,
		sanitizer:      sanitizer,
	}
}

// SubmitZipkinBatch方法记录Zipkin Thrift格式的trace数据
func (h *zipkinSpanHandler) SubmitZipkinBatch(ctx thrift.Context, spans []*zipkincore.Span) ([]*zipkincore.Response, error) {
	mSpans := make([]*model.Span, 0, len(spans))
	// 这个不能和jaeger thrift一样批量处理trace包
	// 把接收到的span转化为model.Span
	for _, span := range spans {
		sanitized := h.sanitizer.Sanitize(span)
		mSpans = append(mSpans, convertZipkinToModel(sanitized, h.logger)...)
	}
	// 交给span processor统一处理，并发送到channel队列，供消费者消费存储model.Span
	bools, err := h.modelProcessor.ProcessSpans(mSpans, ZipkinFormatType)
	if err != nil {
		h.logger.Error("Collector failed to process Zipkin span batch", zap.Error(err))
		return nil, err
	}
	responses := make([]*zipkincore.Response, len(spans))
	for i, ok := range bools {
		res := zipkincore.NewResponse()
		res.Ok = ok
		responses[i] = res
	}

	h.logger.Debug("Zipkin span batch processed by the collector.", zap.Int("span-count", len(spans)))
	return responses, nil
}

// ConvertZipkinToModel方法用于把trace由zipkincore.Span格式数据转换为model.Span格式数据
func convertZipkinToModel(zSpan *zipkincore.Span, logger *zap.Logger) []*model.Span {
	mSpans, err := zipkin.ToDomainSpan(zSpan)
	if err != nil {
		logger.Warn("Warning while converting zipkin to domain span", zap.Error(err))
	}
	return mSpans
}
