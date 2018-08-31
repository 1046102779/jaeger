# Jaeger Agent

`jaeger-agent` is a daemon program that runs on every host and receives
tracing information submitted by applications via Jaeger client 
libraries.

## Structure

* Agent
    * processor as ThriftProcessor
        * server as TBufferedServer
            * Thrift UDP Transport
        * reporter as TCollectorReporter
    * sampling server
        * sampling manager as sampling.TCollectorProxy

### UDP Server

Listens on UDP transport, reads data as `[]byte` and forwards to
`processor` over channel. Processor has N workers that read from
the channel, convert to thrift-generated object model, and pass on
to the Reporter. `TCollectorReporter` submits the spans to remote
`tcollector` service.

### Sampling Server

An HTTP server handling request in the form

    http://localhost:port/sampling?service=xxxx`

Delegates to `sampling.Manager` to get the sampling strategy.
`sampling.TCollectorProxy` implements `sampling.Manager` by querying
remote `tcollector` service. Then the server converts
thrift response from sampling manager into JSON and responds to clients.


### 目录说明
1. httpserver: 作为agent server，用于给该agent所在的host上的微服务，提供获取采样策略、Baggage携带信息字段服务
  - manager.go 提供服务接口声明(采样策略服务，Baggage携带信息字段列表)
  - collector_proxy.go 作为agent client，微服务通过这个client获取这两个服务
  - server.go 作为agent server的实现, 采样策略提供了两种：一、采样；二、流控

2. agent.go: 提供获取Agent实例，并通过Run方法运行多个worker processes和http server， 前者用于接收client的udp trace包，并作为生产者把trace包发送到channel队列上，然后多个worker processes通过竞争从这个channel队列上获取trace包，并push到collector server

3. builder.go: 提供用于agent需要的相关参数，包括worker processes参数、http server相关参数、collector server peers服务发现等, 并创建各个实例, 并把相关参数值设置到Builder实例中

3. flags.go: 设置默认的worker processes相关参数值, http server相关参数, Collector server相关参数
