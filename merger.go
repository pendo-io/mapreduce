package mapreduce

import (
	"container/heap"
)

type mappedDataList struct {
	data    []MappedData
	compare KeyHandler
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

type mappedDataMergeItem struct {
	iterator IntermediateStorageIterator
	datum    MappedData
}

type mappedDataMerger struct {
	items   []mappedDataMergeItem
	compare KeyHandler
	inited  bool
}

func (a *mappedDataMerger) Len() int { return len(a.items) }
func (a *mappedDataMerger) Less(i, j int) bool {
	return a.compare.Less(a.items[i].datum.Key, a.items[j].datum.Key)
}
func (a *mappedDataMerger) Swap(i, j int)      { a.items[i], a.items[j] = a.items[j], a.items[i] }
func (a *mappedDataMerger) Push(x interface{}) { a.items = append(a.items, x.(mappedDataMergeItem)) }
func (a *mappedDataMerger) Pop() interface{} {
	x := a.items[len(a.items)-1]
	a.items = a.items[0 : len(a.items)-1]
	return x
}

func (merger *mappedDataMerger) next() (*MappedData, error) {
	if len(merger.items) == 0 {
		return nil, nil
	}

	if !merger.inited {
		heap.Init(merger)
		merger.inited = true
	}

	item := heap.Pop(merger).(mappedDataMergeItem)

	if newItem, exists, err := item.iterator.Next(); err != nil {
		return nil, err
	} else if exists {
		heap.Push(merger, mappedDataMergeItem{item.iterator, newItem})
	}

	return &item.datum, nil
}

func (merger *mappedDataMerger) empty() bool {
	return len(merger.items) == 0
}

func newMerger(comparator KeyHandler) *mappedDataMerger {
	return &mappedDataMerger{
		items:   make([]mappedDataMergeItem, 0),
		compare: comparator,
	}
}

func (merger *mappedDataMerger) addSource(iterator IntermediateStorageIterator) error {
	firstItem, exists, err := iterator.Next()
	if err != nil {
		return err
	} else if exists {
		merger.items = append(merger.items, mappedDataMergeItem{iterator, firstItem})
	}

	return nil
}
