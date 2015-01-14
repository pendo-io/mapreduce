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
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

type spillStruct struct {
	contents      []byte
	linesPerShard []int
}

func writeSpill(c appengine.Context, handler KeyValueHandler, dataSets []mappedDataList) (spillStruct, error) {
	spill := spillStruct{
		linesPerShard: make([]int, len(dataSets)),
	}

	buf := &bytes.Buffer{}
	writer := gzip.NewWriter(buf)

	for i, dataSet := range dataSets {
		sort.Sort(dataSet)

		for _, item := range dataSet.data {
			key := handler.KeyDump(item.Key)
			value, err := handler.ValueDump(item.Value)
			if err != nil {
				return spillStruct{}, fmt.Errorf("error dumping item value: %s", err)
			}

			if bytes.Equal(key, value) {
				key = nil
			}

			// since these are writing into a byte buffer they can't really fail
			binary.Write(writer, binary.LittleEndian, int32(len(key)))
			binary.Write(writer, binary.LittleEndian, int32(len(value)))
			writer.Write(key)
			writer.Write(value)
		}

		spill.linesPerShard[i] = len(dataSets[i].data)
	}

	writer.Close()
	spill.contents = buf.Bytes()

	return spill, nil
}

type spillIterator struct {
	r             io.ReadCloser
	lineCount     int
	shard         int
	linesPerShard []int
	handler       KeyValueHandler
}

func (si *spillIterator) Next() (MappedData, bool, error) {
	if si.lineCount == si.linesPerShard[si.shard] {
		return MappedData{}, false, nil
	}

	si.lineCount++

	var keyLen, valueLen int32

	if err := binary.Read(si.r, binary.LittleEndian, &keyLen); err != nil {
		return MappedData{}, false, fmt.Errorf("error reading spill key length: %s", err)
	} else if err := binary.Read(si.r, binary.LittleEndian, &valueLen); err != nil {
		return MappedData{}, false, fmt.Errorf("error reading spill value length: %s", err)
	}

	keyBytes := make([]byte, keyLen)
	valueBytes := make([]byte, valueLen)

	var m MappedData

	if _, err := io.ReadFull(si.r, keyBytes); err != nil {
		return MappedData{}, false, fmt.Errorf("error reading spill key: %s", err)
	} else if _, err := io.ReadFull(si.r, valueBytes); err != nil {
		return MappedData{}, false, fmt.Errorf("error reading spill value: %s", err)
	} else if m.Value, err = si.handler.ValueLoad(valueBytes); err != nil {
		return MappedData{}, false, fmt.Errorf("error loading spill value: %s", err)
	} else if keyLen == 0 {
		m.Key = m.Value
	} else if m.Key, err = si.handler.KeyLoad(keyBytes); err != nil {
		return MappedData{}, false, fmt.Errorf("error loading spill key: %s", err)
	}

	return m, true, nil
}

func (si *spillIterator) Close() error {
	return si.r.Close()
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

func newSpillIterator(contents []byte, linesPerShard []int, handler KeyValueHandler) spillIterator {
	r, _ := gzip.NewReader(bytes.NewBuffer(contents))

	return spillIterator{
		r:             r,
		linesPerShard: linesPerShard,
		handler:       handler,
	}
}

type spillMerger struct {
	spillIters []spillIterator
	nextShard  int
	shardCount int
	handler    KeyValueHandler
}

func spillSetMerger(c appengine.Context, spills []spillStruct, handler KeyValueHandler) (*spillMerger, error) {
	if len(spills) == 0 {
		return nil, nil
	}

	merger := &spillMerger{
		spillIters: make([]spillIterator, len(spills)),
		shardCount: len(spills[0].linesPerShard),
		handler:    handler,
	}

	for spill := range spills {
		merger.spillIters[spill] = newSpillIterator(spills[spill].contents, spills[spill].linesPerShard, handler)
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

func mergeSpills(c appengine.Context, intStorage IntermediateStorage, handler KeyValueHandler, spills []spillStruct) ([]string, error) {
	if len(spills) == 0 {
		return []string{}, nil
	}

	spillMerger, err := spillSetMerger(c, spills, handler)
	if err != nil {
		return nil, fmt.Errorf("failed to create spill merger: %s", err)
	}

	numShards := len(spills[0].linesPerShard)
	names := make([]string, 0, numShards)
	for shardCount := 0; shardCount < numShards; shardCount++ {
		c.Infof("merging shard %d/%d", shardCount, numShards)
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

	return names, nil
}
