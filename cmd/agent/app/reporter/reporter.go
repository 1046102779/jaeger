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

package reporter

import (
	"github.com/jaegertracing/jaeger/pkg/multierror"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
)

// Reporter处理有processor接收的trace包，并通过processor的handler把trace包发送给collector server
// 至于EmitZipkinBatch与EmitBatch方法的区别，一个是Zipkin格式，一个是Jaeger自定义格式
type Reporter interface {
	EmitZipkinBatch(spans []*zipkincore.Span) (err error)
	EmitBatch(batch *jaeger.Batch) (err error)
}

// MultiReporter用于trace包发送给多个collector server, 多个collector存储
type MultiReporter []Reporter

// NewMultiReporter方法用于创建一个MultiReporter实例，指定了多个Reporter，也就是collector clients
func NewMultiReporter(reps ...Reporter) MultiReporter {
	return reps
}

// EmitZipkinBatch方法用于批量调用EmitZipkinBatch, 对于每一个Reporter传输的实现，都可以使用自己的方式
func (mr MultiReporter) EmitZipkinBatch(spans []*zipkincore.Span) error {
	var errors []error
	for _, rep := range mr {
		if err := rep.EmitZipkinBatch(spans); err != nil {
			errors = append(errors, err)
		}
	}
	return multierror.Wrap(errors)
}

// EmitBatch方法用于批量调用EmitBatch， 对于每个Reporter都有自己的实现
func (mr MultiReporter) EmitBatch(batch *jaeger.Batch) error {
	var errors []error
	for _, rep := range mr {
		if err := rep.EmitBatch(batch); err != nil {
			errors = append(errors, err)
		}
	}
	return multierror.Wrap(errors)
}
