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

func (s *mappedDataMerger) next() (MappedData, error) {
	if !s.inited {
		heap.Init(s)
		s.inited = true
	}

	item := heap.Pop(s).(mappedDataMergeItem)

	if newItem, exists, err := item.iterator.Next(); err != nil {
		return MappedData{}, err
	} else if exists {
		heap.Push(s, mappedDataMergeItem{item.iterator, newItem})
	}

	return item.datum, nil
}
