package kyrie

import (
	"fmt"
	"strconv"
)

type IntermediateStorageIterator interface {
	Next() (MappedData, bool, error)
}

type IntermediateStorage interface {
	Store(items []MappedData, keyHandler KeyHandler, valueHandler ValueHandler) (string, error)
	Iterator(name string) (IntermediateStorageIterator, error)
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

func (m *MemoryIntermediateStorage) Store(items []MappedData, keyHandler KeyHandler, valueHandler ValueHandler) (string, error) {
	name := fmt.Sprintf("%d", len(m.items))
	m.items = append(m.items, items)
	return name, nil
}

func (m *MemoryIntermediateStorage) Iterator(name string) (IntermediateStorageIterator, error) {
	index, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, err
	}

	return &ArrayIterator{m.items[index], 0}, nil
}
