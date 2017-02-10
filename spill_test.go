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
	"fmt"
	ck "gopkg.in/check.v1"
)

func (mrt *MapreduceTests) TestSpill(c *ck.C) {
	memStorage := &memoryIntermediateStorage{}

	handler := struct {
		Int64KeyHandler
		StringValueHandler
	}{}

	// this creates 5 spills, each with 5 shards. the first spill has values 0...2000, and each shard in the first spill contains
	//   1995 ... 10, 5, 0
	//   1996 ... 11, 6, 1
	//   1997 ... 12, 7, 2
	//   1998 ... 13, 8, 3
	//   1999 ... 14, 9, 4
	//
	// (similary for the rest of the spills)
	//
	// we test that we can merge each of the individual shards together from the various spills. with sorting, that makes each
	// merged shard like 0, 5, 10, ...., 9990, 9995

	spills := make([]spillStruct, 5)
	for pass := 0; pass < 5; pass++ {
		dataSets := make([]mappedDataList, 5)
		for shard := range dataSets {
			dataSets[shard] = mappedDataList{data: make([]MappedData, 0), compare: handler}
		}

		for i := 2000*(pass+1) - 1; i >= 2000*pass; i-- {
			shard := i % 5
			dataSets[shard].data = append(dataSets[shard].data, MappedData{Key: int64(i), Value: fmt.Sprintf("%s", i)})
		}

		spill, err := writeSpill(nil, handler, dataSets)
		c.Assert(err, ck.IsNil)
		c.Assert(spill.linesPerShard, ck.DeepEquals, []int{400, 400, 400, 400, 400})

		spills[pass] = spill
	}

	names, err := mergeSpills(nil, memStorage, handler, spills, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(len(names), ck.Equals, 5)

	for shardCount := 0; shardCount < 5; shardCount++ {
		iter, err := memStorage.Iterator(nil, names[shardCount], handler)
		c.Assert(err, ck.IsNil)

		for i := 0; i < 2000; i++ {
			item, exists, err := iter.Next()
			c.Assert(err, ck.IsNil)
			c.Assert(exists, ck.Equals, true)
			c.Assert(item, ck.NotNil)
			c.Assert(item.Key, ck.Equals, int64(i*5+shardCount))
		}

		_, exists, err := iter.Next()
		c.Assert(err, ck.IsNil)
		c.Assert(exists, ck.Equals, false)
	}

	c.Assert(len(memStorage.items), ck.Equals, 5)

}
