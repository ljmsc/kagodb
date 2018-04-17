package kagodb

import (
	"github.com/Shopify/sarama"
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"os/signal"
	"log"
	"fmt"
)

type Topic struct {
	Name     string
	Metadata *sarama.TopicMetadata
	K        *Kagodb
	Store    map[uint64][]byte
	close    chan bool
}

func NewTopic(name string, metadata *sarama.TopicMetadata, kagodb *Kagodb) *Topic {
	topic := new(Topic)
	topic.Name = name
	topic.Metadata = metadata
	topic.K = kagodb
	topic.Store = make(map[uint64][]byte)
	return topic
}

func ConvertIntKey(key uint64) (sarama.ByteEncoder, error) {
	var byteKey sarama.ByteEncoder

	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(buf, key)
	byteKey = buf

	return byteKey, nil
}

func ConvertByteKey(byteKey sarama.ByteEncoder) (uint64, error) {
	r := bytes.NewReader(byteKey)
	key, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}

	return key, nil
}

func (t *Topic) updateStore(key uint64, data []byte) error {
	if len(data) == 0 {
		delete(t.Store, key)
		return nil
	}
	t.Store[key] = data
	return nil
}

func (t *Topic) StartConsumer() error {
	t.close = make(chan bool, 1)
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt)

	for _, partition := range t.Metadata.Partitions {
		partitionConsumer, err := t.K.Consumer.ConsumePartition(t.Name, partition.ID, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		go func() {
			defer partitionConsumer.Close()
		partitionLoop:
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					key, err := ConvertByteKey(msg.Key)
					if err != nil {
						log.Fatalf("can't convert key (%s)", err.Error())
					}

					err2 := t.updateStore(key, msg.Value)
					if err2 != nil {
						log.Fatalf("can't update local store (%s)", err.Error())
					}

				case <-interruptSignal:
					break partitionLoop
				case <-t.close:
					break partitionLoop
				}
			}

		}()
	}
	return nil
}

func (t *Topic) Close() error {
	t.close <- true
	return nil
}

func (t *Topic) getNextId() uint64 {
	//todo: impl with store
	for {
		i := rand.Uint64()
		if _, ok := t.Store[i]; !ok {
			return i
		}
	}
}

func (t *Topic) CreateItem(data []byte) (uint64, error) {
	newKey := t.getNextId()
	return newKey, t.UpdateItem(newKey, data)
}

func (t *Topic) ReadItem(key uint64) ([]byte, error) {
	if data, ok := t.Store[key]; ok {
		return data, nil
	}
	return nil, nil
}

func (t *Topic) UpdateItem(key uint64, data []byte) error {
	var keyData, valueData sarama.ByteEncoder
	keyData, err := ConvertIntKey(key)
	if err != nil {
		return err
	}

	valueData = data

	msg := sarama.ProducerMessage{Topic: t.Name, Key: keyData, Value: valueData}
	// synchron update
	if t.K.Config.SynchronizedUpdates {
		_, _, err := t.K.SyncProducer.SendMessage(&msg)
		if err != nil {
			return err
		}

		t.updateStore(key, data)
		return nil
	}
	// asynchron update
	t.K.AsyncProducer.Input() <- &msg
	return nil
}

func (t *Topic) DeleteItem(key uint64) error {
	return t.UpdateItem(key, []byte{})
}

func (t *Topic) FindItem(validate func([]byte) interface{}) []interface{} {
	collector := make([]interface{}, 0, int(len(t.Store)/2))
	for _, itemData := range t.Store {
		item := validate(itemData)
		if item != nil {
			collector = append(collector, item)
		}
	}
	return collector
}

// Debug function only
func (t *Topic) PrintLocalItemStore(cast func([]byte) string) {
	fmt.Println("Start printing local store items:")
	for key, data := range t.Store {
		dataString := cast(data)
		fmt.Printf("%d: %s \n", key, dataString)
	}
	fmt.Println("End printing")
}
