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

	"github.com/pkg/errors"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/tchannel-go"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/pkg/discovery"
	"github.com/jaegertracing/jaeger/pkg/discovery/peerlistmgr"
)

const (
	defaultMinPeers = 3

	agentServiceName            = "jaeger-agent"
	defaultCollectorServiceName = "jaeger-collector"
)

// Builder类似于Agent Builder，用于构建一个或者多个服务，组装服务所需要的各个组件，如：Processor server需要配置、需要创建Processor实例，并且把相关实例参数写入到Builder中
//
// 这个Builder主要用于Collector server的静态服务列表，或者动态服务发现的配置、实例相关构建
type Builder struct {
	// collector server静态服务端口列表
	CollectorHostPorts []string `yaml:"collectorHostPorts"`

	// 用于连接collector servers实例的数量，默认为3个。
	//
	// 这个说明collector servers至少有三个实例
	DiscoveryMinPeers int `yaml:"minPeers"`

	// collector服务名称
	CollectorServiceName string `yaml:"collectorServiceName"`

	// 连接的超时时间设置
	ConnCheckTimeout time.Duration

	discoverer discovery.Discoverer
	notifier   discovery.Notifier
	channel    *tchannel.Channel
}

// NewBuilder方法创建一个Builder实例
func NewBuilder() *Builder {
	return &Builder{}
}

// WithDiscoverer方法用于设置Builder的属性值: 服务发现
func (b *Builder) WithDiscoverer(d discovery.Discoverer) *Builder {
	b.discoverer = d
	return b
}

// WithDiscoveryNotifier方法用于设置服务发现监听的key对应的value值发生了变化，事件通知
func (b *Builder) WithDiscoveryNotifier(n discovery.Notifier) *Builder {
	b.notifier = n
	return b
}

// WithCollectorServiceName方法用于设置collector server服务名称
func (b *Builder) WithCollectorServiceName(s string) *Builder {
	b.CollectorServiceName = s
	return b
}

// WithChannel方法用于设置获取相关服务Peers的channel
func (b *Builder) WithChannel(c *tchannel.Channel) *Builder {
	b.channel = c
	return b
}

// enableDiscovery方法通过tchannel获取collector service列表，并监听这些列表values值的变化, 并返回一个管理collector services的实例
func (b *Builder) enableDiscovery(channel *tchannel.Channel, logger *zap.Logger) (*peerlistmgr.PeerListManager, error) {
	if b.discoverer == nil && b.notifier == nil {
		return nil, nil
	}
	if b.discoverer == nil || b.notifier == nil {
		return nil, errors.New("both discovery.Discoverer and discovery.Notifier must be specified")
	}

	logger.Info("Enabling service discovery", zap.String("service", b.CollectorServiceName))

	// tchannel通过collector service获取peers
	subCh := channel.GetSubChannel(b.CollectorServiceName, tchannel.Isolated)
	peers := subCh.Peers()
	// 创建PeerListManager实例
	return peerlistmgr.New(peers, b.discoverer, b.notifier,
		peerlistmgr.Options.MinPeers(defaultInt(b.DiscoveryMinPeers, defaultMinPeers)),
		peerlistmgr.Options.Logger(logger),
		peerlistmgr.Options.ConnCheckTimeout(b.ConnCheckTimeout),
	)
}

// CreateReporter方法用于创建Reporter实例，Reporter可以发送trace包到collector servers中
func (b *Builder) CreateReporter(mFactory metrics.Factory, logger *zap.Logger) (*Reporter, error) {
	// 如果tchannel为空，则直接新建channel实例
	if b.channel == nil {
		// ignore errors since it only happens on empty service name
		b.channel, _ = tchannel.NewChannel(agentServiceName, nil)
	}

	if b.CollectorServiceName == "" {
		b.CollectorServiceName = defaultCollectorServiceName
	}

	// 如果CollectorHostPorts不为空，则是指定的静态服务发现collector service的host:port列表
	if len(b.CollectorHostPorts) != 0 {
		d := discovery.FixedDiscoverer(b.CollectorHostPorts)
		b = b.WithDiscoverer(d).WithDiscoveryNotifier(&discovery.Dispatcher{})
	}

	// 默认动态服务发现
	//
	// 1. 订阅服务发现对collector service的host:port监听事件，注册监听事件
	// 2. 获取collector service peers
	peerListMgr, err := b.enableDiscovery(b.channel, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot enable service discovery")
	}
	// 创建Reporter实例，获取collector client, 两个client，分别是：jaeger, zipkin
	return New(b.CollectorServiceName, b.channel, peerListMgr, mFactory, logger), nil
}

func defaultInt(value int, defaultVal int) int {
	if value == 0 {
		value = defaultVal
	}
	return value
}
