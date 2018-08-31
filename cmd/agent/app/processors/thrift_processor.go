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

package processors

import (
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/agent/app/customtransports"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers"
)

// ThriftProcessor用于指定thrift协议的processor服务, 这个processor server提供udp端口服务，获取业务微服务的trace，并通过processor workers作为消费者抢占channel队列上的trace包，并发送到collector servers上(通过channel获取collector peers)
type ThriftProcessor struct {
	// 提供udp端口服务, 用于获取client的trace包
	server servers.Server
	// 已经在Builder实例中通过每个Processor的协议类型thrift、jaeger
	handler       AgentProcessor
	protocolPool  *sync.Pool
	numProcessors int
	processing    sync.WaitGroup
	logger        *zap.Logger
	metrics       struct {
		// Amount of time taken for processor to close
		ProcessorCloseTimer metrics.Timer `metric:"thrift.udp.t-processor.close-time"`

		// Number of failed buffer process operations
		HandlerProcessError metrics.Counter `metric:"thrift.udp.t-processor.handler-errors"`
	}
}

// AgentProcessor interface用于发送trace包到collector server
type AgentProcessor interface {
	Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException)
}

// NewThriftProcessor方法创建一个ThriftProcessor实例
func NewThriftProcessor(
	server servers.Server,
	numProcessors int,
	mFactory metrics.Factory,
	factory thrift.TProtocolFactory,
	handler AgentProcessor,
	logger *zap.Logger,
) (*ThriftProcessor, error) {
	if numProcessors <= 0 {
		return nil, fmt.Errorf(
			"Number of processors must be greater than 0, called with %d", numProcessors)
	}
	var protocolPool = &sync.Pool{
		New: func() interface{} {
			trans := &customtransport.TBufferedReadTransport{}
			return factory.GetProtocol(trans)
		},
	}

	res := &ThriftProcessor{
		server:        server,
		handler:       handler,
		protocolPool:  protocolPool,
		logger:        logger,
		numProcessors: numProcessors,
	}
	metrics.Init(&res.metrics, mFactory, nil)
	return res, nil
}

// Serve方法:
// 1. 启动多个消费者workers，用于抢占channel队列上的trace包
// 2. 启动http server服务，用于给业务微服务提供采样策略和Baggage携带信息字段列表
func (s *ThriftProcessor) Serve() {
	s.processing.Add(s.numProcessors)
	for i := 0; i < s.numProcessors; i++ {
		go s.processBuffer()
	}

	s.server.Serve()
}

// IsServing indicates whether the server is currently serving traffic
func (s *ThriftProcessor) IsServing() bool {
	return s.server.IsServing()
}

// Stop方法停止监控metrics相关指标，http server停止，消费者服务列表停止
// emptied by the readers
func (s *ThriftProcessor) Stop() {
	stopwatch := metrics.StartStopwatch(s.metrics.ProcessorCloseTimer)
	s.server.Stop()
	s.processing.Wait()
	stopwatch.Stop()
}

// processBuffer方法用于每个消费者抢占channel队列上的trace包
func (s *ThriftProcessor) processBuffer() {
	// 每个processor worker阻塞等待channel队列上的trace包
	for readBuf := range s.server.DataChan() {
		// 获取临时对象池的内存空间, 读取trace包，并写入到该内存空间
		protocol := s.protocolPool.Get().(thrift.TProtocol)
		payload := readBuf.GetBytes()
		protocol.Transport().Write(payload)
		s.logger.Debug("Span(s) received by the agent", zap.Int("bytes-received", len(payload)))
		// 待处理的trace包数量-1
		s.server.DataRecd(readBuf) // acknowledge receipt and release the buffer

		// ThriftProcessor已经封装了调用collector server所需要的client（通过channel获取collector service peers）
		// 发起thrift rpc调用，把trace包，也就是payload发送到collector server
		if ok, _ := s.handler.Process(protocol, protocol); !ok {
			// TODO log the error
			s.metrics.HandlerProcessError.Inc(1)
		}
		s.protocolPool.Put(protocol)
	}
	s.processing.Done()
}
