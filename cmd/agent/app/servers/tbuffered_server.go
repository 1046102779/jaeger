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

package servers

import (
	"sync"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/jaeger-lib/metrics"
)

// TBufferedServer用于读取微服务发送过来的trace包数据的相关存储参数
type TBufferedServer struct {
	// NB. queueLength HAS to be at the top of the struct or it will SIGSEV for certain architectures.
	// See https://github.com/golang/go/issues/13868
	queueSize     int64         // 当前未处理的trace包数量
	dataChan      chan *ReadBuf // 每次udp server接收到trace包后，通过channel队列发送给消费者
	maxPacketSize int           // 传输能够接收的最大trace包size
	maxQueueSize  int           // 每个processer udp server最多能接收的trace包数量
	serving       uint32
	transport     thrift.TTransport // udp server
	readBufPool   *sync.Pool
	metrics       struct {
		// Size of the current server queue
		QueueSize metrics.Gauge `metric:"thrift.udp.server.queue_size"`

		// Size (in bytes) of packets received by server
		PacketSize metrics.Gauge `metric:"thrift.udp.server.packet_size"`

		// Number of packets dropped by server
		PacketsDropped metrics.Counter `metric:"thrift.udp.server.packets.dropped"`

		// Number of packets processed by server
		PacketsProcessed metrics.Counter `metric:"thrift.udp.server.packets.processed"`

		// Number of malformed packets the server received
		ReadError metrics.Counter `metric:"thrift.udp.server.read.errors"`
	}
}

// NewTBufferedServer方法用于创建一个TBufferedServer实例,
// 通过这个实例进行udp server监听数据读取
func NewTBufferedServer(
	transport thrift.TTransport,
	maxQueueSize int,
	maxPacketSize int,
	mFactory metrics.Factory,
) (*TBufferedServer, error) {
	// channel队列能够接收的最多trace包数量为maxQueueSize
	dataChan := make(chan *ReadBuf, maxQueueSize)

	// 设置临时对象池，用于获取存放trace包所需要的内存
	var readBufPool = &sync.Pool{
		New: func() interface{} {
			return &ReadBuf{bytes: make([]byte, maxPacketSize)}
		},
	}

	res := &TBufferedServer{
		dataChan:      dataChan,
		transport:     transport,
		maxQueueSize:  maxQueueSize,
		maxPacketSize: maxPacketSize,
		readBufPool:   readBufPool,
	}
	// 初始化metrics指标
	metrics.Init(&res.metrics, mFactory, nil)
	return res, nil
}

// Serve方法用于初始化ReadBuf，并开启udp server监听
func (s *TBufferedServer) Serve() {
	atomic.StoreUint32(&s.serving, 1)
	for s.IsServing() {
		readBuf := s.readBufPool.Get().(*ReadBuf)
		// 阻塞读取微服务发过来的trace包数据
		n, err := s.transport.Read(readBuf.bytes)
		if err == nil {
			readBuf.n = n
			s.metrics.PacketSize.Update(int64(n))
			select {
			// trace包发送到channel队列，等待processor workers消费者消费
			case s.dataChan <- readBuf:
				s.metrics.PacketsProcessed.Inc(1)
				s.updateQueueSize(1)
			default:
				s.metrics.PacketsDropped.Inc(1)
			}
		} else {
			s.metrics.ReadError.Inc(1)
		}
	}
}

// 更新发送到队列上还没处理的数据
func (s *TBufferedServer) updateQueueSize(delta int64) {
	atomic.AddInt64(&s.queueSize, delta)
	s.metrics.QueueSize.Update(atomic.LoadInt64(&s.queueSize))
}

// IsServing方法用于校验udp server是否要退出
func (s *TBufferedServer) IsServing() bool {
	return atomic.LoadUint32(&s.serving) == 1
}

// Stop 方法用于退出udp server监听, 并关闭channel队列
func (s *TBufferedServer) Stop() {
	atomic.StoreUint32(&s.serving, 0)
	s.transport.Close()
	close(s.dataChan)
}

// DataChan方法用于每个processor worker的阻塞等待channel队列上trace包的到来
func (s *TBufferedServer) DataChan() chan *ReadBuf {
	return s.dataChan
}

// DataRecd方法用于当process worker消费者处理完一个trace包后，就表示待处理的trace包数量-1
// 并把trace包释放到临时对象池
func (s *TBufferedServer) DataRecd(buf *ReadBuf) {
	s.updateQueueSize(-1)
	s.readBufPool.Put(buf)
}
