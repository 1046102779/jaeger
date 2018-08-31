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
	"fmt"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/agent/app/httpserver"
	"github.com/jaegertracing/jaeger/cmd/agent/app/processors"
	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	tchreporter "github.com/jaegertracing/jaeger/cmd/agent/app/reporter/tchannel"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers"
	"github.com/jaegertracing/jaeger/cmd/agent/app/servers/thriftudp"
	jmetrics "github.com/jaegertracing/jaeger/pkg/metrics"
	zipkinThrift "github.com/jaegertracing/jaeger/thrift-gen/agent"
	jaegerThrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
)

const (
	defaultQueueSize        = 1000
	defaultMaxPacketSize    = 65000
	defaultServerWorkers    = 10
	defaultMinPeers         = 3
	defaultConnCheckTimeout = 250 * time.Millisecond

	defaultHTTPServerHostPort = ":5778"

	jaegerModel Model = "jaeger"
	zipkinModel       = "zipkin"

	compactProtocol Protocol = "compact"
	binaryProtocol           = "binary"
)

// Model 数据传输方式：jaeger、zipkin
type Model string

// Protocol 数据传入协议：compact、binary
type Protocol string

var (
	errNoReporters = errors.New("agent requires at least one Reporter")

	protocolFactoryMap = map[Protocol]thrift.TProtocolFactory{
		compactProtocol: thrift.NewTCompactProtocolFactory(),
		binaryProtocol:  thrift.NewTBinaryProtocolFactoryDefault(),
	}
)

// Builder用于构建Processors、HTTPServer等服务所需要的配置解析和创建相关服务
type Builder struct {
	Processors []ProcessorConfiguration `yaml:"processors"`
	HTTPServer HTTPServerConfiguration  `yaml:"httpServer"`
	Metrics    jmetrics.Builder         `yaml:"metrics"`

	tchreporter.Builder `yaml:",inline"`

	otherReporters []reporter.Reporter
	metricsFactory metrics.Factory
}

// ProcessorConfiguration用于jaeger、zipkin的配置, 包括端口
type ProcessorConfiguration struct {
	Workers  int                 `yaml:"workers"`
	Model    Model               `yaml:"model"`
	Protocol Protocol            `yaml:"protocol"`
	Server   ServerConfiguration `yaml:"server"`
}

// ServerConfiguration用于上面的ProcessorConfiguration属性
// Queue队列大小、Packet包大小
type ServerConfiguration struct {
	QueueSize     int    `yaml:"queueSize"`
	MaxPacketSize int    `yaml:"maxPacketSize"`
	HostPort      string `yaml:"hostPort" validate:"nonzero"`
}

// HTTPServerConfiguration用于HTTPServer的服务端口
type HTTPServerConfiguration struct {
	HostPort string `yaml:"hostPort" validate:"nonzero"`
}

// WithReporter adds auxiliary reporters.
func (b *Builder) WithReporter(r reporter.Reporter) *Builder {
	b.otherReporters = append(b.otherReporters, r)
	return b
}

// WithMetricsFactory sets an externally initialized metrics factory.
func (b *Builder) WithMetricsFactory(mf metrics.Factory) *Builder {
	b.metricsFactory = mf
	return b
}

// 创建collect client, 它可能包含多个remote collector peers, 通过tchannel库实现
func (b *Builder) createMainReporter(mFactory metrics.Factory, logger *zap.Logger) (*tchreporter.Reporter, error) {
	return b.CreateReporter(mFactory, logger)
}

func (b *Builder) getMetricsFactory() (metrics.Factory, error) {
	if b.metricsFactory != nil {
		return b.metricsFactory, nil
	}

	baseFactory, err := b.Metrics.CreateMetricsFactory("jaeger")
	if err != nil {
		return nil, err
	}

	return baseFactory.Namespace("agent", nil), nil
}

// CreateAgent方法用于创建agent，包括collector client、processors和httpServer
func (b *Builder) CreateAgent(logger *zap.Logger) (*Agent, error) {
	mFacTory, err := b.getMetricsFactory()
	if err != nil {
		return nil, errors.Wrap(err, "cannot create metrics factory")
	}
	mainReporter, err := b.createMainReporter(mFactory, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create main Reporter")
	}
	var rep reporter.Reporter = mainReporter
	if len(b.otherReporters) > 0 {
		reps := append([]reporter.Reporter{mainReporter}, b.otherReporters...)
		rep = reporter.NewMultiReporter(reps...)
	}
	processors, err := b.GetProcessors(rep, mFactory, logger)
	if err != nil {
		return nil, err
	}
	httpServer := b.HTTPServer.GetHTTPServer(b.CollectorServiceName, mainReporter.Channel(), mFactory)
	if h := b.Metrics.Handler(); mFactory != nil && h != nil {
		httpServer.Handler.(*http.ServeMux).Handle(b.Metrics.HTTPRoute, h)
	}
	return NewAgent(processors, httpServer, logger), nil
}

// GetProcessors方法用于创建多个processors worker，并在每个processor中带有collector client(reporter)
// 每个processor server通过udp port监听微服务发过来的trace包, 并通过collector peers发送给collect server
func (b *Builder) GetProcessors(rep reporter.Reporter, mFactory metrics.Factory, logger *zap.Logger) ([]processors.Processor, error) {
	retMe := make([]processors.Processor, len(b.Processors))
	for idx, cfg := range b.Processors {
		protoFactory, ok := protocolFactoryMap[cfg.Protocol]
		if !ok {
			return nil, fmt.Errorf("cannot find protocol factory for protocol %v", cfg.Protocol)
		}
		// 根据jaeger、zipkin类型获取具体的handler实例, 因为不同的数据类型，则有不同的处理，并改造为collect server通用接收的数据格式
		var handler processors.AgentProcessor
		switch cfg.Model {
		case jaegerModel:
			handler = jaegerThrift.NewAgentProcessor(rep)
		case zipkinModel:
			handler = zipkinThrift.NewAgentProcessor(rep)
		default:
			return nil, fmt.Errorf("cannot find agent processor for data model %v", cfg.Model)
		}
		metrics := mFactory.Namespace("", map[string]string{
			"protocol": string(cfg.Protocol),
			"model":    string(cfg.Model),
		})
		// 初始化每个Processor，包括创建udp transport, 每个processor最大能够处理的trace包数量和这些trace包累积的最大size, 每个Processor启动的goroutine数量，
		// 一个Processor server通过udp监听trace包，并通过每个Processor的10个goroutines，通过channel队列抢占处理trace包
		processor, err := cfg.GetThriftProcessor(metrics, protoFactory, handler, logger)
		if err != nil {
			return nil, err
		}
		retMe[idx] = processor
	}
	return retMe, nil
}

// GetHTTPServer方法作为agent http server接收业务微服务的请求，用于获取采样策略、Baggage携带字段列表
// agent http server的采样策略和Baggage携带字段列表，是通过CollectProxy获取collector server的采样策略和Baggage携带字段列表
// route: / , /sampling 和 /baggageRestrictions
func (c HTTPServerConfiguration) GetHTTPServer(svc string, channel *tchannel.Channel, mFactory metrics.Factory) *http.Server {
	mgr := httpserver.NewCollectorProxy(svc, channel, mFactory)
	if c.HostPort == "" {
		c.HostPort = defaultHTTPServerHostPort
	}
	return httpserver.NewHTTPServer(c.HostPort, mgr, mFactory)
}

// GetThriftProcessor方法通过Process配置，获取udp transport，以及初始化Processor
func (c *ProcessorConfiguration) GetThriftProcessor(
	mFactory metrics.Factory,
	factory thrift.TProtocolFactory,
	handler processors.AgentProcessor,
	logger *zap.Logger,
) (processors.Processor, error) {
	c.applyDefaults()

	server, err := c.Server.getUDPServer(mFactory)
	if err != nil {
		return nil, err
	}

	return processors.NewThriftProcessor(server, c.Workers, mFactory, factory, handler, logger)
}

// 设置Process的Worker默认数量，如果worker数量为0，则设置默认值；否则不变
func (c *ProcessorConfiguration) applyDefaults() {
	c.Workers = defaultInt(c.Workers, defaultServerWorkers)
}

// 设置Processor server通过udp接收微服务的trace包相关参数配置, 作用同上
// 两个参数：
// 1. QueueSize: 最大接收多少个未处理的trace包数量
// 2. MaxPacketSize: 未处理包总共的trace包总size
func (c *ServerConfiguration) applyDefaults() {
	c.QueueSize = defaultInt(c.QueueSize, defaultQueueSize)
	c.MaxPacketSize = defaultInt(c.MaxPacketSize, defaultMaxPacketSize)
}

// getUDPServer方法通过udp初始化一个udp transport，端口为host:port
// 并返回servers.Server, 对外提供指定服务的采样策略和指定服务的Baggage携带字段
func (c *ServerConfiguration) getUDPServer(mFactory metrics.Factory) (servers.Server, error) {
	c.applyDefaults()

	if c.HostPort == "" {
		return nil, fmt.Errorf("no host:port provided for udp server: %+v", *c)
	}
	transport, err := thriftudp.NewTUDPServerTransport(c.HostPort)
	if err != nil {
		return nil, err
	}

	return servers.NewTBufferedServer(transport, c.QueueSize, c.MaxPacketSize, mFactory)
}

func defaultInt(value int, defaultVal int) int {
	if value == 0 {
		value = defaultVal
	}
	return value
}
