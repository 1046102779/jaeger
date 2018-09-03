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
	"io/ioutil"
	"mime"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/gorilla/mux"
	tchanThrift "github.com/uber/tchannel-go/thrift"

	tJaeger "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
)

const (
	// UnableToReadBodyErrFormat is an error message for invalid requests
	UnableToReadBodyErrFormat = "Unable to process request body: %v"
)

var (
	acceptedThriftFormats = map[string]struct{}{
		"application/x-thrift":                 {},
		"application/vnd.apache.thrift.binary": {},
	}
)

// APIHandler用于接收client通过http发送的trace包
//
// 注意：agent processor是通过tchannel框架发送的trace包，tcp连接
type APIHandler struct {
	jaegerBatchesHandler JaegerBatchesHandler
}

// NewAPIHandler方法创建一个APIHandler实例, 这里只接收jaeger compact协议格式
func NewAPIHandler(
	jaegerBatchesHandler JaegerBatchesHandler,
) *APIHandler {
	return &APIHandler{
		jaegerBatchesHandler: jaegerBatchesHandler,
	}
}

// RegisterRoutes方法用于路由注册
// http method: post
// http route: /api/traces
// http handler: APIHandler saveSpan
func (aH *APIHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/api/traces", aH.saveSpan).Methods(http.MethodPost)
}

// 处理route: /api/traces post
// 接收http trace数据，并发送给span_process.go进行数据格式转换和存储
func (aH *APIHandler) saveSpan(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, fmt.Sprintf(UnableToReadBodyErrFormat, err), http.StatusInternalServerError)
		return
	}

	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))

	if err != nil {
		http.Error(w, fmt.Sprintf("Cannot parse content type: %v", err), http.StatusBadRequest)
		return
	}

	if _, ok := acceptedThriftFormats[contentType]; !ok {
		http.Error(w, fmt.Sprintf("Unsupported content type: %v", contentType), http.StatusBadRequest)
		return
	}

	tdes := thrift.NewTDeserializer()
	// (NB): We decided to use this struct instead of straight batches to be as consistent with tchannel intake as possible.
	batch := &tJaeger.Batch{}
	if err = tdes.Read(batch, bodyBytes); err != nil {
		http.Error(w, fmt.Sprintf(UnableToReadBodyErrFormat, err), http.StatusBadRequest)
		return
	}
	ctx, cancel := tchanThrift.NewContext(time.Minute)
	defer cancel()
	batches := []*tJaeger.Batch{batch}
	// 构建批量trace, 并转化为jaeger compact格式, 然后进行存储
	if _, err = aH.jaegerBatchesHandler.SubmitBatches(ctx, batches); err != nil {
		http.Error(w, fmt.Sprintf("Cannot submit Jaeger batch: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
