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
	"fmt"
	"io"
	"sort"
)

type spill struct {
	intermediateName string
	linesPerShard    []int
}

func writeSpill(c appengine.Context, intermediate IntermediateStorage, handler KeyValueHandler, dataSets []mappedDataList) (spill, error) {
	outFile, err := intermediate.CreateIntermediate(c, handler)
	if err != nil {
		return spill{}, fmt.Errorf("error creating spill file: %s", err)
	}

	spill := spill{
		linesPerShard: make([]int, len(dataSets)),
	}

	for i := range dataSets {
		sort.Sort(dataSets[i])

		for itemIdx := range dataSets[i].data {
			if err := outFile.WriteMappedData(dataSets[i].data[itemIdx]); err != nil {
				outFile.Close(c)
				return spill, fmt.Errorf("error writing to spill: %s", err)
			}
		}

		spill.linesPerShard[i] = len(dataSets[i].data)
	}

	if err := outFile.Close(c); err != nil {
		return spill, fmt.Errorf("failed to close spill file: %s", err)
	}

	spill.intermediateName = outFile.ToName()

	return spill, nil
}

type spillIterator struct {
	iter          IntermediateStorageIterator
	lineCount     int
	shard         int
	linesPerShard []int
}

func (si *spillIterator) Next() (MappedData, bool, error) {
	if si.lineCount == si.linesPerShard[si.shard] {
		return MappedData{}, false, nil
	}

	si.lineCount++
	return si.iter.Next()
}

func (si *spillIterator) Close() error {
	return si.iter.Close()
}

func (si *spillIterator) NextShard() error {
	if si.shard >= len(si.linesPerShard) {
		return io.EOF
	}

	if si.lineCount != si.linesPerShard[si.shard] {
		panic("cannot go to next shard in spill until entire shard has been consumed")
	}

	si.shard++
	si.lineCount = 0
	if si.shard >= len(si.linesPerShard) {
		return io.EOF
	}

	return nil
}

func newSpillIterator(iter IntermediateStorageIterator, linesPerShard []int) spillIterator {
	return spillIterator{
		iter:          iter,
		linesPerShard: linesPerShard,
	}
}

type spillMerger struct {
	spillIters []spillIterator
	nextShard  int
	shardCount int
	handler    KeyValueHandler
}

func spillSetMerger(c appengine.Context, spills []spill, intStorage IntermediateStorage, handler KeyValueHandler) (*spillMerger, error) {
	if len(spills) == 0 {
		return nil, nil
	}

	merger := &spillMerger{
		spillIters: make([]spillIterator, len(spills)),
		shardCount: len(spills[0].linesPerShard),
		handler:    handler,
	}

	for spill := range spills {
		if iter, err := intStorage.Iterator(c, spills[spill].intermediateName, handler); err != nil {
			return nil, fmt.Errorf("error opening spill: %s", err)
		} else {
			merger.spillIters[spill] = newSpillIterator(iter, spills[spill].linesPerShard)
		}
	}

	return merger, nil
}

func (merger *spillMerger) nextMerger() (int, *mappedDataMerger, error) {
	if merger.nextShard >= merger.shardCount {
		panic(fmt.Sprintf("asking for too many shards from spill (%d)", merger.shardCount))
	} else if merger.nextShard > 0 {
		for i := range merger.spillIters {
			if err := merger.spillIters[i].NextShard(); err != nil {
				panic(fmt.Sprintf("NextShard() failed in nextMerger(): %s", err))
			}
		}
	}

	mm := newMerger(merger.handler)
	for i := range merger.spillIters {
		if err := mm.addSource(&merger.spillIters[i]); err != nil {
			return -1, nil, fmt.Errorf("error adding iterator to merger: %s", err)
		}
	}

	merger.nextShard++

	return merger.nextShard - 1, mm, nil
}

func mergeSpills(c appengine.Context, intStorage IntermediateStorage, handler KeyValueHandler, spills []spill) ([]string, error) {
	if len(spills) == 0 {
		return []string{}, nil
	}

	spillMerger, err := spillSetMerger(c, spills, intStorage, handler)
	if err != nil {
		return nil, fmt.Errorf("failed to create spill merger: %s", err)
	}

	numShards := len(spills[0].linesPerShard)
	names := make([]string, 0, numShards)
	for shardCount := 0; shardCount < numShards; shardCount++ {
		if shard, merger, err := spillMerger.nextMerger(); err != nil {
			return nil, fmt.Errorf("failed to create merger for shard %d: %s", shardCount, err)
		} else if shard != shardCount {
			panic("lost track of shard count for spill merge!!!")
		} else if name, err := mergeIntermediate(c, intStorage, handler, merger); err != nil {
			return nil, fmt.Errorf("failed to merge shard %d: %s", shardCount, err)
		} else {
			names = append(names, name)
		}
	}

	for spill := range spills {
		if err := intStorage.RemoveIntermediate(c, spills[spill].intermediateName); err != nil {
			c.Infof("failed to remove intermediate file %s: %s", spills[spill].intermediateName, err)
		}
	}

	return names, nil
}
