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

import "io"

// Server为agent processors的udp server，用于接收微服务发送过来的trace
type Server interface {
	Serve()
	IsServing() bool
	Stop()
	DataChan() chan *ReadBuf
	DataRecd(*ReadBuf) // must be called by consumer after reading data from the ReadBuf
}

// ReadBuf为一次性读取，不会分批次读数据，这个为udp server上接收到的trace包内存数据
type ReadBuf struct {
	bytes []byte
	n     int
}

// GetBytes方法用于返回当前trace数据
func (r *ReadBuf) GetBytes() []byte {
	return r.bytes[:r.n]
}

// Read方法用于读取所有数据到传入的比特流中，如果传入的内存引用空间不够，则直接截取
//
// 这个ReadBuf虽然可以多次读取，但一般是用来一次性读取的，因为反复读取都是相同内容
func (r *ReadBuf) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, io.EOF
	}
	n := r.n
	copied := copy(p, r.bytes[:n])
	r.n -= copied
	return n, nil
}
