package kyrie

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

type KeyValueHandler interface {
	KeyHandler
	ValueHandler
}

type IntermediateStorageIterator interface {
	Next() (MappedData, bool, error)
}

type IntermediateStorage interface {
	Store(items []MappedData, handler KeyValueHandler) (string, error)
	Iterator(name string, handler KeyValueHandler) (IntermediateStorageIterator, error)
}

type ArrayIterator struct {
	data      []MappedData
	nextIndex int
}

func (sf *ArrayIterator) Next() (MappedData, bool, error) {
	if sf.nextIndex >= len(sf.data) {
		return MappedData{}, false, nil
	}

	sf.nextIndex++
	return sf.data[sf.nextIndex-1], true, nil
}

type MemoryIntermediateStorage struct {
	items [][]MappedData
}

func (m *MemoryIntermediateStorage) Store(items []MappedData, handler KeyValueHandler) (string, error) {
	name := fmt.Sprintf("%d", len(m.items))
	m.items = append(m.items, items)
	return name, nil
}

func (m *MemoryIntermediateStorage) Iterator(name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	index, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, err
	}

	return &ArrayIterator{m.items[index], 0}, nil
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

type FileIntermediateStorage struct {
	PathPattern string
	count       int
}

func (fis *FileIntermediateStorage) Store(items []MappedData, handler KeyValueHandler) (string, error) {
	name := fmt.Sprintf(fis.PathPattern, fmt.Sprintf("%d", fis.count))
	f, err := os.Create(name)
	if err != nil {
		return "", err
	}
	fis.count++

	if err := copyItemsToWriter(items, handler, f); err != nil {
		os.Remove(name)
		return "", err
	}

	return name, f.Close()
}

func (fis *FileIntermediateStorage) Iterator(name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &ReaderIterator{bufio.NewReader(f), handler}, nil
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
