package kagodb

import (
	"math/rand"
	"errors"
	"github.com/Shopify/sarama"
)

type Config struct {
	// true is kafka producer connection should be synchron, false for asynchron
	SynchronizedUpdates bool
	// node used for broker connection. if this string is empty a random node from Nodes field will be used
	BrokerNode string
	// nodes used for the consumer and producer connection
	Nodes []string

	// config for sarama broker connection
	SaramaConfig *sarama.Config
}

// return node for broker connection
func (c *Config) GetBrokerNode() string {
	if c.BrokerNode != "" {
		return c.BrokerNode
	}
	randInt := rand.Intn(len(c.Nodes))
	return c.Nodes[randInt]
}

// return error if config is not complete for correct usage
func (c *Config) Validate() error {
	if len(c.Nodes) < 1 {
		return errors.New("node list is empty")
	}
	
	return nil
}
