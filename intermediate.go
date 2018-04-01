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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/net/context"
)

type KeyValueHandler interface {
	KeyHandler
	ValueHandler
}

// this overlaps a great deal with SingleOutputWriter; they often share an implementation
type SingleIntermediateStorageWriter interface {
	WriteMappedData(data MappedData) error
	Close(c context.Context) error
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
	CreateIntermediate(c context.Context, handler KeyValueHandler) (SingleIntermediateStorageWriter, error)
	Iterator(c context.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error)
	RemoveIntermediate(c context.Context, name string) error
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
// with no encoding. It only works in test environments. And barely there.
type memoryIntermediateStorage struct {
	items    map[string][]MappedData
	nextFile int
}

func (m *memoryIntermediateStorage) add(name string, data MappedData) {
	m.items[name] = append(m.items[name], data)
}

func (m *memoryIntermediateStorage) CreateIntermediate(c context.Context, handler KeyValueHandler) (SingleIntermediateStorageWriter, error) {
	if m.items == nil {
		m.items = make(map[string][]MappedData)
	}

	name := fmt.Sprintf("%d", m.nextFile)
	m.nextFile++
	return &memoryIntermediateStorageWriter{name, m}, nil
}

func (m *memoryIntermediateStorage) Iterator(c context.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	if _, exists := m.items[name]; !exists {
		return nil, os.ErrNotExist
	}
	return &arrayIterator{m.items[name], 0}, nil
}

func (m *memoryIntermediateStorage) RemoveIntermediate(c context.Context, name string) error {
	// eh. whatever.
	delete(m.items, name)
	return nil
}

type memoryIntermediateStorageWriter struct {
	name    string
	storage *memoryIntermediateStorage
}

func (w *memoryIntermediateStorageWriter) Close(c context.Context) error { return nil }

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

func mergeIntermediate(w SingleIntermediateStorageWriter, handler KeyValueHandler, merger *mappedDataMerger) error {
	rows := 0
	for !merger.empty() {
		item, err := merger.next()
		if err != nil {
			return err
		}

		if err := w.WriteMappedData(*item); err != nil {
			return err
		}

		rows++
	}

	return nil
}
