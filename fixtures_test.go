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
	"net/http"
	"testing"

	"github.com/pendo-io/appwrap"
	"golang.org/x/net/context"
	"gopkg.in/check.v1"
)

type MapreduceTests struct {
	nullLog appwrap.NullLogger
}

func (mrt *MapreduceTests) ContextFn(*http.Request) context.Context {
	return appwrap.StubContext()
}

var _ = check.Suite(&MapreduceTests{})

func TestMapReduce(t *testing.T) { check.TestingT(t) }

func (s *MapreduceTests) SetUpSuite(c *check.C) {

}

func (s *MapreduceTests) TearDownSuite(c *check.C) {
}
