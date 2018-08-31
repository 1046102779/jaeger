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

package customtransport

import (
	"bytes"

	"github.com/apache/thrift/lib/go/thrift"
)

// TBufferedReadTransport是一个用于读取网络数据的bytes.Buffer
type TBufferedReadTransport struct {
	readBuf *bytes.Buffer
}

// NewTBufferedReadTransport方法用于创建一个TBufferedReadTransport实例，存储网络传输内存数据,
// 这个应该是用来存储本地host上的微服务发送的trace网络数据
func NewTBufferedReadTransport(readBuf *bytes.Buffer) (*TBufferedReadTransport, error) {
	return &TBufferedReadTransport{readBuf: readBuf}, nil
}

// IsOpen does nothing as transport is not maintaining the connection
// Required to maintain thrift.TTransport interface
func (p *TBufferedReadTransport) IsOpen() bool {
	return true
}

// Open does nothing as transport is not maintaining the connection
// Required to maintain thrift.TTransport interface
func (p *TBufferedReadTransport) Open() error {
	return nil
}

// Close does nothing as transport is not maintaining the connection
// Required to maintain thrift.TTransport interface
func (p *TBufferedReadTransport) Close() error {
	return nil
}

// Read方法用于读取长度为len(buf)的数据存储到buf中
func (p *TBufferedReadTransport) Read(buf []byte) (int, error) {
	in, err := p.readBuf.Read(buf)
	return in, thrift.NewTTransportExceptionFromError(err)
}

// RemainingBytes方法用于返回剩余可读内存数据长度
func (p *TBufferedReadTransport) RemainingBytes() uint64 {
	return uint64(p.readBuf.Len())
}

// Write方法命名并不好，它其实是重新指定一块内存区域
//
// Write方法字面理解应该是追加数据
func (p *TBufferedReadTransport) Write(buf []byte) (int, error) {
	p.readBuf = bytes.NewBuffer(buf)
	return len(buf), nil
}

// Flush does nothing as udp server does not write responses back
// Required to maintain thrift.TTransport interface
func (p *TBufferedReadTransport) Flush() error {
	return nil
}
