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
	ck "gopkg.in/check.v1"
	"math/rand"
	"time"
)

func (mrt *MapreduceTests) TestIntermediateMerge(c *ck.C) {
	memStorage := &memoryIntermediateStorage{}

	handler := struct {
		Int64KeyHandler
		Int64ValueHandler
	}{}

	rand.Seed(int64(time.Now().Nanosecond()))

	names := make([]string, 0)
	for i := 0; i < 5; i++ {
		w, _ := memStorage.CreateIntermediate(mrt.Context, handler)
		for j := 0; j < 10000; j++ {
			w.WriteMappedData(MappedData{Key: int64(j * i), Value: int64(i)})
		}

		w.Close(mrt.Context)
		names = append(names, w.ToName())
	}

	name, err := mergeIntermediate(mrt.Context, memStorage, handler, names)
	c.Assert(err, ck.IsNil)
	c.Assert(len(memStorage.items), ck.Equals, 1)

	iter, err := memStorage.Iterator(mrt.Context, name, handler)
	c.Assert(err, ck.IsNil)

	next := 0
	for data, done, err := iter.Next(); !done && err == nil; data, done, err = iter.Next() {
		c.Assert(data.Key, ck.Equals, next)
		next++
	}

}
