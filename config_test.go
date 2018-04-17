package kagodb

import "testing"

var testNodes = []string{"test:9092", "test2:9092"}

func TestConfig_GetBrokerNode(t *testing.T) {
	c := Config{Nodes: testNodes}
	node := c.GetBrokerNode()
	if node == "" {
		t.Fatal("node name is empty")
	}

	for _, nodeName := range testNodes {
		if node == nodeName {
			return
		}
	}
	t.Errorf("node name (%s) is not in node list", node)
}

func TestConfig_GetBrokerNode2(t *testing.T) {
	brokerTestNode := "brokerNode:9092"
	c := Config{Nodes: testNodes, BrokerNode: brokerTestNode}
	broker := c.GetBrokerNode()
	if broker != brokerTestNode {
		t.Errorf("wrong value for broker name - %s != %s", broker, brokerTestNode)
	}
}

func TestConfig_Validate(t *testing.T) {
	c := Config{Nodes: testNodes}
	err := c.Validate()
	if err != nil {
		t.Errorf("config is not valid (%s)", err.Error())
	}
}

func TestConfig_Validate2(t *testing.T) {
	c := Config{}
	err := c.Validate()
	if err == nil {
		t.Error("config should not be valid")
	}
}
