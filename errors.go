package kagodb

import "fmt"

type ConfigMissing struct {
}

func (cm *ConfigMissing) Error() string {
	return "no config defined"
}

type MetadataRequestFailed struct {
	NestedError error
}

func (mrf *MetadataRequestFailed) Error() string {
	if mrf.NestedError == nil {
		return "can't request metadata for topic"
	}
	return fmt.Sprintf("can't request metadata for topic (%s)", mrf.NestedError.Error())
}

type ItemNotFound struct {
}

func (inf *ItemNotFound) Error() string {
	return "item not found"
}
