package mapreduce

import (
	"fmt"
	"strconv"
)

// ValueHandler converts values from a map step to []byte and back again
type ValueHandler interface {
	ValueDump(a interface{}) ([]byte, error)
	ValueLoad([]byte) (interface{}, error)
}

// StringValueHandler provides a ValueHandler for string values
type StringValueHandler struct{}

func (j StringValueHandler) ValueDump(a interface{}) ([]byte, error) {
	return []byte(a.(string)), nil
}

func (j StringValueHandler) ValueLoad(val []byte) (interface{}, error) {
	return string(val), nil
}

// StringValueHandler provides a ValueHandler for int values
type IntValueHandler struct{}

func (j IntValueHandler) ValueDump(a interface{}) ([]byte, error) {
	return []byte(fmt.Sprintf("%d", a)), nil
}

func (j IntValueHandler) ValueLoad(val []byte) (interface{}, error) {
	value, err := strconv.ParseInt(string(val), 10, 64)
	return int(value), err
}
