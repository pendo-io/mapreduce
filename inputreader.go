package kyrie

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type InputReader interface {
	Split() ([]SingleInputReader, error)
	ReaderFromName(name string) (SingleInputReader, error)
}

type SingleInputReader interface {
	Next() (interface{}, error)
	ToName() string
}

type SingleFileLineInputReader struct {
	path string
	r    *bufio.Reader
}

type FileLineInputReader struct {
	Paths []string
}

func (m FileLineInputReader) Split() ([]SingleInputReader, error) {
	readers := make([]SingleInputReader, len(m.Paths))
	for i := range m.Paths {
		var err error
		readers[i], err = newSingleFileLineInputReader(m.Paths[i])
		if err != nil {
			return nil, err
		}
	}

	return readers, nil
}

func (m FileLineInputReader) ReaderFromName(path string) (SingleInputReader, error) {
	return newSingleFileLineInputReader(path)
}

func newSingleFileLineInputReader(path string) (SingleFileLineInputReader, error) {
	reader, err := os.Open(path)
	if err != nil {
		return SingleFileLineInputReader{}, err
	}

	return SingleFileLineInputReader{path, bufio.NewReader(reader)}, nil
}

func (ir SingleFileLineInputReader) ToName() string {
	return ir.path
}

func (ir SingleFileLineInputReader) String() string {
	return fmt.Sprintf("SingleFileLineInputReader(%s)", ir.path)
}

func (ir SingleFileLineInputReader) Split() ([]SingleInputReader, error) {
	return []SingleInputReader{ir}, nil
}

func (ir SingleFileLineInputReader) Next() (interface{}, error) {
	s, err := ir.r.ReadString('\n')
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return "", err
	}

	return s[0 : len(s)-1], nil
}
