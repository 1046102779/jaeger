# jaeger demo使用手册

这个是直接在本地运行单个服务，服务包括：Cassandra服务(docker运行)，collector服务，agent服务和hotrod示例

注意：jaeger-ui需要与query做一个代理3000:16686的指向，这个默认在jaeger-ui中的package.json中proxy指明

## collector

```shell
## 启动docker cassandra服务, 本地服务HORT:PORT = 127.0.0.1:9042
docker run -d --name cassandra -p 9042:9042 daocloud.io/library/cassandra

## 安装TablePlus工具，作为Cassandra GUI工具
## 然后再创建keyspace 
CREATE  KEYSPACE  jaeger_v1_test
WITH replication={'class':'SimpleStrategy', 'replication_factor' : 1};

## 启动collector服务, collector有三个服务端口{tchannel port: 14267, collector http port: 14268, zipkin http port: 9411}, 后端存储Cassandra存储端口：9042
## 其中tchannel为tcp传输
cd ~/godev/src/github.com/jaegertracing/jaeger/cmd/collector
go build
./collector
``` 


## query

```shell
 cd $GOPATH/src/github.com/jaegertracing/jaeger
 
 make build_ui ## 注意，这里需要安装yarn
 
 cd jaeger-ui/packages/jaeger-ui
 
 yarn start ## 启动服务端口：3000
 
## 访问web-ui：http://127.0.0.1:3000, 即可看到jaeger-ui
```

## agent

```shell
 ./agent --collector.host-port=:14267
```

## hotrod

```shell
## 示例演示
./hotrod all
```

## 后续

如果jaeger服务单个部署遇到问题，可以直接email找我
