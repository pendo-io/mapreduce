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
	"appengine/aetest"
	"gopkg.in/check.v1"
	"net/http"
	"testing"
)

type MapreduceTests struct {
	Context aetest.Context
}

func (mrt *MapreduceTests) ContextFn(*http.Request) appengine.Context {
	return mrt.Context
}

var _ = check.Suite(&MapreduceTests{})

func TestMapReduce(t *testing.T) { check.TestingT(t) }

func (s *MapreduceTests) SetUpSuite(c *check.C) {
	if context, err := aetest.NewContext(nil); err != nil {
		c.Fatal(err)
	} else {
		s.Context = context
	}

}

func (s *MapreduceTests) TearDownSuite(c *check.C) {
	s.Context.Close()
}
