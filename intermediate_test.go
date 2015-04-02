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
	"github.com/pendo-io/appwrap"
	ck "gopkg.in/check.v1"
)

func (mrt *MapreduceTests) TestIntermediateMerge(c *ck.C) {
	memStorage := &memoryIntermediateStorage{}
	ctx := appwrap.StubContext()

	handler := struct {
		Int64KeyHandler
		Int64ValueHandler
	}{}

	merger := newMerger(handler)
	for i := 0; i < 5; i++ {
		w, _ := memStorage.CreateIntermediate(ctx, handler)
		for j := 0; j < 1000; j++ {
			w.WriteMappedData(MappedData{Key: int64(j*5 + i), Value: int64(i)})
		}

		w.Close(ctx)

		iterator, _ := memStorage.Iterator(ctx, w.ToName(), handler)
		merger.addSource(iterator)
	}

	w, _ := memStorage.CreateIntermediate(ctx, handler)
	err := mergeIntermediate(w, handler, merger)
	c.Assert(err, ck.Equals, nil)
	err = w.Close(ctx)
	c.Assert(err, ck.Equals, nil)

	iter, err := memStorage.Iterator(ctx, w.ToName(), handler)
	c.Assert(err, ck.IsNil)

	next := int64(0)
	for data, valid, err := iter.Next(); valid && err == nil; data, valid, err = iter.Next() {
		c.Assert(data.Key, ck.Equals, next)
		next++
	}

	c.Assert(next, ck.Equals, int64(5000))

}
