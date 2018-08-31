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

package tchannel

import (
	"time"

	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/discovery/peerlistmgr"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
)

const (
	jaegerBatches = "jaeger"
	zipkinBatches = "zipkin"
)

type batchMetrics struct {
	// Number of successful batch submissions to collector
	BatchesSubmitted metrics.Counter `metric:"batches.submitted"`

	// Number of failed batch submissions to collector
	BatchesFailures metrics.Counter `metric:"batches.failures"`

	// Number of spans in a batch submitted to collector
	BatchSize metrics.Gauge `metric:"batch_size"`

	// Number of successful span submissions to collector
	SpansSubmitted metrics.Counter `metric:"spans.submitted"`

	// Number of failed span submissions to collector
	SpansFailures metrics.Counter `metric:"spans.failures"`
}

// Reporter forwards received spans to central collector tier over TChannel.
// Reporter用于把processor从channel队列上接收trace, 通过tchannel发送到collector servers
// 因为Reporter实现了Reporter interface：EmitZipkinBatch和EmitBatch方法
// 前者用于zipkin数据格式进行rpc调用，后者用于jaeger数据格式进行rpc调用
type Reporter struct {
	channel        *tchannel.Channel
	zClient        zipkincore.TChanZipkinCollector
	jClient        jaeger.TChanCollector
	peerListMgr    *peerlistmgr.PeerListManager
	batchesMetrics map[string]batchMetrics
	logger         *zap.Logger
}

// New方法用于创建一个Reporter实例，并分别获取jaeger、thrift协议解析的client: zClient和jClient
func New(
	collectorServiceName string,
	channel *tchannel.Channel,
	peerListMgr *peerlistmgr.PeerListManager,
	mFactory metrics.Factory,
	zlogger *zap.Logger,
) *Reporter {
	// 通过channel获取collector service的client，并分别封装为zClient, jClient
	thriftClient := thrift.NewClient(channel, collectorServiceName, nil)
	// zClient提供了SubmitZipkinBatch方法调用，最终通过client.Call进行rpc调用
	// ZipkinCollector server
	zClient := zipkincore.NewTChanZipkinCollectorClient(thriftClient)
	// jClient提供了SubmitBatches方法调用，最终也通过client.Call进行rpc调用
	// default collector server
	jClient := jaeger.NewTChanCollectorClient(thriftClient)
	batchesMetrics := map[string]batchMetrics{}
	for _, s := range []string{zipkinBatches, jaegerBatches} {
		bm := batchMetrics{}
		metrics.Init(&bm, mFactory.Namespace("tchannel-reporter", map[string]string{"format": s}), nil)
		batchesMetrics[s] = bm
	}
	// 包括channel，zClient，jClient，指标和服务发现peerListMgr
	return &Reporter{
		channel:        channel,
		zClient:        zClient,
		jClient:        jClient,
		peerListMgr:    peerListMgr,
		logger:         zlogger,
		batchesMetrics: batchesMetrics,
	}
}

// Channel方法返回TChannel
func (r *Reporter) Channel() *tchannel.Channel {
	return r.channel
}

// EmitZipkinBatch方法通过rpc调用，把spans传输给ZipkinCollector server
func (r *Reporter) EmitZipkinBatch(spans []*zipkincore.Span) error {
	submissionFunc := func(ctx thrift.Context) error {
		_, err := r.zClient.SubmitZipkinBatch(ctx, spans)
		return err
	}
	// 做一些参数封装，比如：context.Context；以及metrics指标的更新
	return r.submitAndReport(
		submissionFunc,
		"Could not submit zipkin batch",
		int64(len(spans)),
		r.batchesMetrics[zipkinBatches],
	)
}

// EmitBatch方法为默认的rpc调用，jaeger.Batch 传输给jaeger collector server
func (r *Reporter) EmitBatch(batch *jaeger.Batch) error {
	submissionFunc := func(ctx thrift.Context) error {
		_, err := r.jClient.SubmitBatches(ctx, []*jaeger.Batch{batch})
		return err
	}
	// 同上
	return r.submitAndReport(
		submissionFunc,
		"Could not submit jaeger batch",
		int64(len(batch.Spans)),
		r.batchesMetrics[jaegerBatches],
	)
}

func (r *Reporter) submitAndReport(submissionFunc func(ctx thrift.Context) error, errMsg string, size int64, batchMetrics batchMetrics) error {
	ctx, cancel := tchannel.NewContextBuilder(time.Second).DisableTracing().Build()
	defer cancel()

	if err := submissionFunc(ctx); err != nil {
		batchMetrics.BatchesFailures.Inc(1)
		batchMetrics.SpansFailures.Inc(size)
		r.logger.Error(errMsg, zap.Error(err))
		return err
	}

	r.logger.Debug("Span batch submitted by the agent", zap.Int64("span-count", size))
	batchMetrics.BatchSize.Update(size)
	batchMetrics.BatchesSubmitted.Inc(1)
	batchMetrics.SpansSubmitted.Inc(size)
	return nil
}
