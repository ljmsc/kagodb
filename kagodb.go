package kagodb

import (
	"github.com/Shopify/sarama"
	"errors"
)

type Kagodb struct {
	Config        *Config
	Broker        *sarama.Broker
	Consumer      sarama.Consumer
	SyncProducer  sarama.SyncProducer
	AsyncProducer sarama.AsyncProducer
}

func NewKagodb(config *Config) *Kagodb {
	return &(Kagodb{Config: config})
}

// connect to broker, creates consumer and producer. if the connection fails all established connection will be closed
func (k *Kagodb) Open() error {
	if k.Config == nil {
		return &ConfigMissing{}
	}

	err := k.Config.Validate()
	if err != nil {
		return err
	}

	//connect to broker
	err2 := k.connectToBroker()
	if err2 != nil {
		return err2
	}

	//create consumer
	err3 := k.connectToConsumer()
	if err3 != nil {
		//close already established connections
		k.Close()
		return err3
	}

	//create producer
	err4 := k.connectToProducer()
	if err4 != nil {
		//close already established connections
		k.Close()
		return err4
	}

	return nil
}

func (k *Kagodb) connectToBroker() error {
	broker := sarama.NewBroker(k.Config.GetBrokerNode())
	err := broker.Open(k.Config.SaramaConfig)
	if err != nil {
		return err
	}
	k.Broker = broker
	return nil
}

func (k *Kagodb) connectToConsumer() error {
	consumer, err := sarama.NewConsumer(k.Config.Nodes, k.Config.SaramaConfig)
	if err != nil {
		return err
	}
	k.Consumer = consumer
	return nil
}

func (k *Kagodb) connectToProducer() error {
	if k.Config.SynchronizedUpdates {
		return k.connectToSyncProducer()
	}
	return k.connectToAsyncProducer()
}

func (k *Kagodb) connectToSyncProducer() error {
	producer, err := sarama.NewSyncProducer(k.Config.Nodes, k.Config.SaramaConfig)
	if err != nil {
		return err
	}
	k.SyncProducer = producer
	return nil
}

func (k *Kagodb) connectToAsyncProducer() error {
	producer, err := sarama.NewAsyncProducer(k.Config.Nodes, k.Config.SaramaConfig)
	if err != nil {
		return err
	}
	k.AsyncProducer = producer
	return nil
}

func (k *Kagodb) Close() error {

	if k.Consumer != nil {
		err := k.Consumer.Close()
		if err != nil {
			defer k.SyncProducer.Close()
			defer k.AsyncProducer.Close()
			defer k.Broker.Close()
			return err
		}
	}

	if k.SyncProducer != nil {
		err := k.SyncProducer.Close()
		if err != nil {
			defer k.AsyncProducer.Close()
			defer k.Broker.Close()
			return err
		}
	}

	if k.AsyncProducer != nil {
		err := k.AsyncProducer.Close()
		if err != nil {
			defer k.Broker.Close()
			return err
		}
	}

	if k.Broker != nil {
		err := k.Broker.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// creates or open existing topic
func (k *Kagodb) OpenTopic(name string) (*Topic, error) {
	return k.topic(name, true)
}

// open existing topic, fails if the topic dosn't exists
func (k *Kagodb) GetTopic(name string) (*Topic, error) {
	return k.topic(name, false)
}

func (k *Kagodb) topic(name string, create bool) (*Topic, error) {
	metadataRequest := sarama.MetadataRequest{Topics: []string{name}, AllowAutoTopicCreation: create}
	response, err := k.Broker.GetMetadata(&metadataRequest)
	if err != nil {
		return nil, &MetadataRequestFailed{NestedError: err}
	}
	if len(response.Topics) != 1 {
		return nil, errors.New("response is to big or to short")
	}

	topic := NewTopic(name, response.Topics[0], k)
	err2 := topic.StartConsumer()
	if err2 != nil {
		return nil, err2
	}
	return topic, nil
}
