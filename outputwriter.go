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
	"os"
)

type OutputWriter interface {
	WriterNames(c appengine.Context) ([]string, error)
	WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error)
}

// local file output; used for testing
type fileLineOutputWriter struct {
	Paths []string
}

func (m fileLineOutputWriter) WriterNames(c appengine.Context) ([]string, error) {
	return m.Paths, nil
}

func (m fileLineOutputWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	return newSingleFileLineOutputWriter(name)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close(c appengine.Context)
	ToName() string
}

type LineOutputWriter struct {
	w io.Writer
}

func (o LineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

type singleFileLineOutputWriter struct {
	LineOutputWriter
	path string
}

func (o LineOutputWriter) Close(c appengine.Context) {
}

func (o singleFileLineOutputWriter) ToName() string {
	return o.path
}

func newSingleFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return singleFileLineOutputWriter{}, err
	}

	return singleFileLineOutputWriter{path: path, LineOutputWriter: LineOutputWriter{w}}, nil
}

// NilOutputWriter collects output and throws it away. Useful for reduce tasks which only have
// side affects
type NilOutputWriter struct {
	count int
}

func (n NilOutputWriter) WriterNames(c appengine.Context) ([]string, error) {
	result := make([]string, n.count)
	for i := range result {
		result[i] = "(niloutputwriter)"
	}

	return result, nil
}

func (n NilOutputWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	return nilSingleOutputWriter{}, nil
}

type nilSingleOutputWriter struct{}

func (n nilSingleOutputWriter) Write(data interface{}) error {
	return nil
}

func (n nilSingleOutputWriter) Close(c appengine.Context) {
}

func (n nilSingleOutputWriter) ToName() string {
	return "(niloutput)"
}
