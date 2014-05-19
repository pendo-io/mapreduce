package mapreduce

import (
	"crypto/sha1"
)

type KeyHandler interface {
	Less(a, b interface{}) bool
	Equal(a, b interface{}) bool
	Shard(a interface{}, shardCount int) int
	KeyDump(a interface{}) []byte
	KeyLoad([]byte) (interface{}, error)
}

type StringKeyHandler struct{}

func (s StringKeyHandler) KeyDump(a interface{}) []byte {
	return []byte(a.(string))
}

func (s StringKeyHandler) KeyLoad(a []byte) (interface{}, error) {
	return string(a), nil
}

func (s StringKeyHandler) Less(a, b interface{}) bool {
	return a.(string) < b.(string)
}

func (s StringKeyHandler) Equal(a, b interface{}) bool {
	return a.(string) == b.(string)
}

func (s StringKeyHandler) Shard(strInt interface{}, shardCount int) int {
	str := strInt.(string)

	h := sha1.New()
	h.Write([]byte(str))
	sum := h.Sum(nil)
	hashVal := int(sum[0])<<8 | int(sum[1])
	return hashVal % shardCount
}
