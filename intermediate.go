// Copyright 2014 pendo.io
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapreduce

import (
	"appengine"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

type KeyValueHandler interface {
	KeyHandler
	ValueHandler
}

type IntermediateStorageIterator interface {
	// Returns mapped data item, a bool saying if it's valid, and an error if one occurred
	// probably cause use error = EOF instead, but we don't
	Next() (MappedData, bool, error)
}

// IntermediateStorage defines how intermediare results are saved and read. If keys need to be serialized
// KeyValueHandler.Load and KeyValueHandler.Save must be used.
type IntermediateStorage interface {
	Store(c appengine.Context, items []MappedData, handler KeyValueHandler) (string, error)
	Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error)
	RemoveIntermediate(c appengine.Context, name string) error
}

type arrayIterator struct {
	data      []MappedData
	nextIndex int
}

func (sf *arrayIterator) Next() (MappedData, bool, error) {
	if sf.nextIndex >= len(sf.data) {
		return MappedData{}, false, nil
	}

	sf.nextIndex++
	return sf.data[sf.nextIndex-1], true, nil
}

// memoryIntermediateStorage is a simple IntermediateStorage implementation which keeps objects in memory
// with no encoding. It only works in test environments.
type memoryIntermediateStorage struct {
	items [][]MappedData
}

func (m *memoryIntermediateStorage) Store(c appengine.Context, items []MappedData, handler KeyValueHandler) (string, error) {
	name := fmt.Sprintf("%d", len(m.items))
	m.items = append(m.items, items)
	return name, nil
}

func (m *memoryIntermediateStorage) Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	index, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, err
	}

	return &arrayIterator{m.items[index], 0}, nil
}

func (m *memoryIntermediateStorage) RemoveIntermediate(c appengine.Context, name string) error {
	// eh. whatever.
	return nil
}

type fileJsonHolder struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ReaderIterator struct {
	reader  *bufio.Reader
	handler KeyValueHandler
}

func (r *ReaderIterator) Next() (MappedData, bool, error) {
	line, err := r.reader.ReadBytes('\n')
	if err == io.EOF {
		return MappedData{}, false, nil
	}
	if err != nil {
		return MappedData{}, false, err
	}

	var jsonStruct fileJsonHolder
	if err := json.Unmarshal(line, &jsonStruct); err != nil {
		return MappedData{}, false, err
	}

	var m MappedData
	m.Key, err = r.handler.KeyLoad([]byte(jsonStruct.Key))
	if err != nil {
		return MappedData{}, false, err
	}

	m.Value, err = r.handler.ValueLoad([]byte(jsonStruct.Value))
	if err != nil {
		return MappedData{}, false, err
	}

	return m, true, nil
}

func copyItemsToWriter(items []MappedData, handler KeyValueHandler, w io.Writer) error {
	var jsonItem fileJsonHolder
	for i := range items {
		var err error

		jsonItem.Key = string(handler.KeyDump(items[i].Key))
		if value, err := handler.ValueDump(items[i].Value); err != nil {
			return err
		} else {
			jsonItem.Value = string(value)
		}

		bytes, err := json.Marshal(jsonItem)
		if err != nil {
			return err
		}

		bytes = append(bytes, '\n')
		if _, err := w.Write(bytes); err != nil {
			return err
		}
	}

	return nil
}
