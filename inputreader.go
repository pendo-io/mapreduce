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
	"bufio"
	"fmt"
	"io"
	"os"
)

// InputReader is responsible for providing unique names for each of the input
// sources for a job, and creating individual SingleInputReader objects from
// those unique names. The number of unique names for the inputs determines the
// number of map tasks
type InputReader interface {
	// ReaderNames() returns a list of reader instance names;
	ReaderNames() ([]string, error)

	// ReaderFromName() creates the SingleInputReader for the given name
	ReaderFromName(c appengine.Context, name string) (SingleInputReader, error)
}

type SingleInputReader interface {
	Next() (interface{}, error)
}

type singleLineReader struct {
	r *bufio.Reader
}

type singleFileLineInputReader struct {
	singleLineReader
	path string
}

type FileLineInputReader struct {
	Paths []string
}

func (m FileLineInputReader) ReaderNames() ([]string, error) {
	return m.Paths, nil
}

func (m FileLineInputReader) ReaderFromName(c appengine.Context, path string) (SingleInputReader, error) {
	return newSingleFileLineInputReader(path)
}

func newSingleFileLineInputReader(path string) (singleFileLineInputReader, error) {
	reader, err := os.Open(path)
	if err != nil {
		return singleFileLineInputReader{}, err
	}

	return singleFileLineInputReader{
		singleLineReader: singleLineReader{bufio.NewReader(reader)},
		path:             path,
	}, nil
}

func (ir singleFileLineInputReader) String() string {
	return fmt.Sprintf("SingleFileLineInputReader(%s)", ir.path)
}

func (ir singleLineReader) Next() (interface{}, error) {
	s, err := ir.r.ReadString('\n')
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return "", err
	}

	return s[0 : len(s)-1], nil
}
