package kyrie

import (
	"bufio"
	"io"
	"os"
)

type InputReader interface {
	Split() ([]SingleInputReader, error)
}

type SingleInputReader interface {
	Next() (interface{}, error)
}

type FileLineInputReader struct {
	Path string
	r    *bufio.Reader
}

func NewFileLineInputReader(path string) (SingleInputReader, error) {
	reader, err := os.Open(path)
	if err != nil {
		return FileLineInputReader{}, err
	}

	return FileLineInputReader{path, bufio.NewReader(reader)}, nil
}

func (ir FileLineInputReader) Next() (interface{}, error) {
	s, err := ir.r.ReadString('\n')
	if err == io.EOF {
		return "", nil
	} else if err != nil {
		return "", err
	}

	return s, nil
}
