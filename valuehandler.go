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
	"strconv"
)

// ValueHandler converts values from a map step to []byte and back again
type ValueHandler interface {
	ValueDump(a interface{}) ([]byte, error)
	ValueLoad([]byte) (interface{}, error)
}

// StringValueHandler provides a ValueHandler for string values
type StringValueHandler struct{}

func (j StringValueHandler) ValueDump(a interface{}) ([]byte, error) {
	return []byte(a.(string)), nil
}

func (j StringValueHandler) ValueLoad(val []byte) (interface{}, error) {
	return string(val), nil
}

// Int64ValueHandler provides a ValueHandler for int values
type Int64ValueHandler struct{}

func (j Int64ValueHandler) ValueDump(a interface{}) ([]byte, error) {
	return []byte(fmt.Sprintf("%d", a)), nil
}

func (j Int64ValueHandler) ValueLoad(val []byte) (interface{}, error) {
	value, err := strconv.ParseInt(string(val), 10, 64)
	return int(value), err
}
