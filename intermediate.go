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
)

type KeyValueHandler interface {
	KeyHandler
	ValueHandler
}

// this overlaps a great deal with SingleOutputWriter; they often share an implementation
type SingleIntermediateStorageWriter interface {
	WriteMappedData(data MappedData) error
	Close(c appengine.Context) error
	ToName() string
}

type IntermediateStorageIterator interface {
	// Returns mapped data item, a bool saying if it's valid, and an error if one occurred
	// probably cause use error = EOF instead, but we don't
	Next() (MappedData, bool, error)
	Close() error
}

// IntermediateStorage defines how intermediare results are saved and read. If keys need to be serialized
// KeyValueHandler.Load and KeyValueHandler.Save must be used.
type IntermediateStorage interface {
	CreateIntermediate(c appengine.Context, handler KeyValueHandler) (SingleIntermediateStorageWriter, error)
	Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error)
	RemoveIntermediate(c appengine.Context, name string) error
}

type arrayIterator struct {
	data      []MappedData
	nextIndex int
}

func (sf *arrayIterator) Close() error {
	return nil
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
	items map[string][]MappedData
}

func (m *memoryIntermediateStorage) add(name string, data MappedData) {
	m.items[name] = append(m.items[name], data)
}

func (m *memoryIntermediateStorage) CreateIntermediate(c appengine.Context, handler KeyValueHandler) (SingleIntermediateStorageWriter, error) {
	if m.items == nil {
		m.items = make(map[string][]MappedData)
	}

	name := fmt.Sprintf("%d", len(m.items))
	return &memoryIntermediateStorageWriter{name, m}, nil
}

func (m *memoryIntermediateStorage) Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	return &arrayIterator{m.items[name], 0}, nil
}

func (m *memoryIntermediateStorage) RemoveIntermediate(c appengine.Context, name string) error {
	// eh. whatever.
	delete(m.items, name)
	return nil
}

type memoryIntermediateStorageWriter struct {
	name    string
	storage *memoryIntermediateStorage
}

func (w *memoryIntermediateStorageWriter) Close(c appengine.Context) error { return nil }

func (w *memoryIntermediateStorageWriter) WriteMappedData(data MappedData) error {
	w.storage.add(w.name, data)
	return nil
}

func (w *memoryIntermediateStorageWriter) ToName() string {
	return w.name
}

type fileJsonHolder struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ReaderIterator struct {
	bufReader *bufio.Reader
	closer    io.ReadCloser
	handler   KeyValueHandler
}

func NewReaderIterator(reader io.ReadCloser, handler KeyValueHandler) IntermediateStorageIterator {
	return &ReaderIterator{
		bufReader: bufio.NewReader(reader),
		closer:    reader,
		handler:   handler,
	}
}

func (r *ReaderIterator) Close() error {
	return r.closer.Close()
}

func (r *ReaderIterator) Next() (MappedData, bool, error) {
	line, err := r.bufReader.ReadBytes('\n')
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

func mergeIntermediate(c appengine.Context, intStorage IntermediateStorage, handler KeyValueHandler, names []string) (string, error) {
	if len(names) == 0 {
		return "", fmt.Errorf("no files to merge")
	} else if len(names) == 1 {
		return names[0], nil
	}

	toClose := make([]io.Closer, 0, len(names))
	defer func() {
		for _, c := range toClose {
			c.Close()
		}
	}()

	merger := newMerger(handler)

	for _, shardName := range names {
		iterator, err := intStorage.Iterator(c, shardName, handler)
		if err != nil {
			return "", err
		}

		merger.addSource(iterator)
		toClose = append(toClose, iterator)
	}

	w, err := intStorage.CreateIntermediate(c, handler)
	if err != nil {
		return "", err
	}

	rows := 0
	for !merger.empty() {
		item, err := merger.next()
		if err != nil {
			return "", err
		}

		if err := w.WriteMappedData(*item); err != nil {
			return "", err
		}

		rows++
	}

	if err := w.Close(c); err != nil {
		return "", fmt.Errorf("failed to close merge file: %s", err)
	}

	c.Infof("merged %d rows for a single shard", rows)

	for _, shardName := range names {
		if err := intStorage.RemoveIntermediate(c, shardName); err != nil {
			c.Errorf("failed to remove intermediate file: %s", err.Error())
		}
	}

	return w.ToName(), nil
}
