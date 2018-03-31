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
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/net/context"
)

type OutputWriter interface {
	WriterNames(c context.Context) ([]string, error)
	WriterFromName(c context.Context, name string) (SingleOutputWriter, error)
}

// local file output; used for testing
type fileLineOutputWriter struct {
	Paths []string
}

func (m fileLineOutputWriter) WriterNames(c context.Context) ([]string, error) {
	return m.Paths, nil
}

func (m fileLineOutputWriter) WriterFromName(c context.Context, name string) (SingleOutputWriter, error) {
	return newSingleFileLineOutputWriter(name)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close(c context.Context) error
	ToName() string
}

type LineOutputWriter struct {
	w       io.WriteCloser
	handler KeyValueHandler
}

func NewLineOutputWriter(w io.WriteCloser, handler KeyValueHandler) LineOutputWriter {
	return LineOutputWriter{w, handler}
}

func (o LineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

func (o LineOutputWriter) WriteMappedData(item MappedData) error {
	if o.handler == nil {
		return fmt.Errorf("WriteMappedData() called without a KeyValueHandler set")
	}

	var jsonItem fileJsonHolder
	jsonItem.Key = string(o.handler.KeyDump(item.Key))
	if value, err := o.handler.ValueDump(item.Value); err != nil {
		return err
	} else {
		jsonItem.Value = string(value)
	}

	bytes, err := json.Marshal(jsonItem)
	if err != nil {
		return err
	}

	bytes = append(bytes, '\n')
	if _, err := o.w.Write(bytes); err != nil {
		return err
	}

	return nil
}

type singleFileLineOutputWriter struct {
	LineOutputWriter
	path string
}

func (o LineOutputWriter) Close(c context.Context) error {
	return o.w.Close()
}

func (o singleFileLineOutputWriter) ToName() string {
	return o.path
}

func newSingleFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return singleFileLineOutputWriter{}, err
	}

	return singleFileLineOutputWriter{path: path, LineOutputWriter: LineOutputWriter{w: w}}, nil
}

// NilOutputWriter collects output and throws it away. Useful for reduce tasks which only have
// side affects
type NilOutputWriter struct {
	Count int
}

func (n NilOutputWriter) WriterNames(c context.Context) ([]string, error) {
	result := make([]string, n.Count)
	for i := range result {
		result[i] = "(niloutputwriter)"
	}

	return result, nil
}

func (n NilOutputWriter) WriterFromName(c context.Context, name string) (SingleOutputWriter, error) {
	return nilSingleOutputWriter{}, nil
}

type nilSingleOutputWriter struct{}

func (n nilSingleOutputWriter) Write(data interface{}) error {
	return nil
}

func (n nilSingleOutputWriter) Close(c context.Context) error {
	return nil
}

func (n nilSingleOutputWriter) ToName() string {
	return "(niloutput)"
}
