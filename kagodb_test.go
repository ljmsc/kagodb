package kagodb

import (
	"testing"
	"os"
	"log"
	"github.com/Shopify/sarama"
	"time"
)

var kafkaNodes = []string{"localhost:9092"}
var testConnection *Kagodb

func TestMain(m *testing.M) {
	sc := sarama.NewConfig()
	sc.Net.DialTimeout = time.Second * 10
	sc.Producer.Return.Successes = true
	c := Config{Nodes: kafkaNodes, SaramaConfig: sc, SynchronizedUpdates: true}
	testConnection = NewKagodb(&c)
	err := testConnection.Open()
	if err != nil {
		log.Fatalf("can't connect to kafka (%s)", err.Error())
	}
	defer testConnection.Close()

	os.Exit(m.Run())
}

func TestNewKagodb(t *testing.T) {
	t.Parallel()
	k := NewKagodb(nil)
	if k == nil {
		t.Error("instane of kagodb is nil")
	}
}

// test happy path
func TestKagodb_Open(t *testing.T) {
	t.Parallel()
	c := Config{Nodes: kafkaNodes}
	k := NewKagodb(&c)
	if k == nil {
		t.Error("instane of kagodb is nil")
	}
	err := k.Open()
	if err != nil {
		t.Errorf("can't establish connection (%s)", err.Error())
	}

	if k.Broker == nil {
		t.Error("broker is nil after connecting to kafka")
	}

	if k.Consumer == nil {
		t.Error("consumer is nil after connecting to kafka")
	}

	if k.AsyncProducer == nil {
		t.Error("SyncProducer is nil after connecting to kafka")
	}

	k.Close()
}

// test error path
func TestKagodb_Open2(t *testing.T) {
	t.Parallel()
	k := NewKagodb(nil)
	if k == nil {
		t.Error("instane of kagodb is nil")
	}
	err := k.Open()
	if err == nil {
		t.Error("open is successful with empty config")
	}
}

func TestKagodb_Close(t *testing.T) {
	t.Parallel()
	c := Config{Nodes: kafkaNodes}
	k := NewKagodb(&c)
	if k == nil {
		t.Error("instane of kagodb is nil")
	}
	err := k.Open()
	if err != nil {
		t.Errorf("can't establish connection (%s)", err.Error())
	}
	err2 := k.Close()
	if err2 != nil {
		t.Errorf("can't close connection properly (%s)", err2.Error())
	}
}

func TestKagodb_OpenTopic(t *testing.T) {
	topic, err := testConnection.OpenTopic("test")
	if err != nil {
		t.Fatalf("can't open existing topic (%s)", err.Error())
	}

	if topic == nil {
		t.Error("topic is nil")
	}
	topic.Close()
}

func TestKagodb_GetTopic(t *testing.T) {
	topic, err := testConnection.GetTopic("test")
	if err != nil {
		t.Fatalf("can't open existing topic (%s)", err.Error())
	}

	if topic == nil {
		t.Error("topic is nil")
	}
	topic.Close()
}
