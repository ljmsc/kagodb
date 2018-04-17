package kagodb

import (
	"testing"
	"bytes"
	"regexp"
	"fmt"
	"time"
)

var testTopic *Topic
var testItemId, testItemId2, testItemId3 uint64

func TestConvertIntKey(t *testing.T) {
	t.Parallel()
	_, err := ConvertIntKey(123)
	if err != nil {
		t.Fatalf("can't convert int to byte (%s)", err.Error())
	}

}

func TestConvertByteKey(t *testing.T) {
	t.Parallel()
	byteKey, err := ConvertIntKey(123)
	if err != nil {
		t.Fatalf("can't convert int to byte (%s)", err.Error())
	}

	intKey, err2 := ConvertByteKey(byteKey)
	if err2 != nil {
		t.Fatalf("can't convert byte to int (%s)", err2.Error())
	}

	if intKey != 123 {
		t.Fatalf("converted int is not excpeted value (%d != 123)", intKey)
	}
}

func TestTopic_StartConsumer(t *testing.T) {
	now := time.Now()

	randomTopicName := fmt.Sprintf("test_%s", now.Format("20060102150405"))
	topic, err := testConnection.OpenTopic(randomTopicName)
	if err != nil {
		t.Fatalf("can't open topic (%s)", err.Error())
	}

	testTopic = topic
}

func TestTopic_CreateItem(t *testing.T) {
	testData := []byte("data1")
	if testTopic == nil {
		t.Fatalf("testTopic is nil")
	}
	newId, err := testTopic.CreateItem(testData)
	if err != nil {
		t.Fatalf("can't create item (%s)", err.Error())
	}
	testItemId = newId
}

func TestTopic_CreateItem2(t *testing.T) {
	testData := []byte("data2")
	if testTopic == nil {
		t.Fatalf("testTopic is nil")
	}
	newId, err := testTopic.CreateItem(testData)
	if err != nil {
		t.Fatalf("can't create item (%s)", err.Error())
	}
	testItemId2 = newId
}

func TestTopic_CreateItem3(t *testing.T) {
	testData := []byte("test")
	if testTopic == nil {
		t.Fatalf("testTopic is nil")
	}
	newId, err := testTopic.CreateItem(testData)
	if err != nil {
		t.Fatalf("can't create item (%s)", err.Error())
	}
	testItemId3 = newId
}

func TestTopic_FindItem(t *testing.T) {
	if testTopic == nil {
		t.Fatalf("testTopic is nil")
	}
	foundData := testTopic.FindItem(func(data []byte) interface{} {
		buf := bytes.NewBuffer(data)
		dataString := buf.String()
		matched, err := regexp.MatchString("data*", dataString)
		if err == nil && matched {
			return dataString
		}
		return nil
	})

	if len(foundData) == 0 {
		t.Fatalf("couldn't found test items")
	}

	if len(foundData) > 2 {
		t.Fatalf("found more data than expected")
	}

	if len(foundData) == 1 {
		t.Fatalf("only one item was found (%s)", foundData)
	}
}

func TestTopic_ReadItem(t *testing.T) {
	item, err := testTopic.ReadItem(testItemId)
	if err != nil {
		t.Fatalf("couldn't get test item with id %d (%s)", testItemId, err.Error())
	}

	if item == nil {
		t.Fatalf("returned item is nil")
	}
}

func TestTopic_UpdateItem(t *testing.T) {
	testData := []byte("test123")
	err := testTopic.UpdateItem(testItemId3, testData)
	if err != nil {
		t.Fatalf("can not update item with id %d (%s)", testItemId3, err.Error())
	}
}

func TestTopic_ReadItem2(t *testing.T) {
	testData := "test123"
	item, err := testTopic.ReadItem(testItemId3)
	if err != nil {
		t.Fatalf("couldn't get test item with id %d", testItemId3)
	}

	if item == nil {
		t.Fatalf("returned item is nil")
	}

	buf := bytes.NewBuffer(item)
	itemData := buf.String()
	if itemData != testData {
		t.Fatalf("The returned item has wrong value (%s != %s)", itemData, testData)
	}
}

func TestTopic_DeleteItem(t *testing.T) {
	err := testTopic.DeleteItem(testItemId2)
	if err != nil {
		t.Fatalf("can't delete item with id %d (%s)", testItemId2, err.Error())
	}
}

func TestTopic_Close(t *testing.T) {
	err := testTopic.Close()
	if err != nil {
		t.Fatalf("can't close topic properly (%s)", err.Error())
	}
}
