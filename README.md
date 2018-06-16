# kagodb
go library for using kafka as database based on sarama

## Installation
```
go get github.com/ljmsc/kagodb
```

## Usage

### Create new Instance and Open connection

```go
saramaConfig := sarama.NewConfig()
saramaConfig.Net.DialTimeout = time.Second * 10
saramaConfig.Producer.Return.Successes = true
c := Config{Nodes: kafkaNodes, SaramaConfig: saramaConfig, SynchronizedUpdates: true}
instance := NewKagodb(&c)
err := instance.Open()
if err != nil {
    log.Fatalf("can't connect to kafka (%s)", err.Error())
}
...

err2 := instance.Close()
if err2 != nil {
    t.Errorf("can't close connection properly (%s)", err2.Error())
}
```


### Open Topic
```go
topic, err := instance.OpenTopic("test_topic")
if err != nil {
    t.Fatalf("can't open topic (%s)", err.Error())
}
defer topic.Close()
```