package kyrie

import (
	"bufio"
	"fmt"
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
	path string
	r    *bufio.Reader
}

func NewFileLineInputReader(path string) (FileLineInputReader, error) {
	reader, err := os.Open(path)
	if err != nil {
		return FileLineInputReader{}, err
	}

	return FileLineInputReader{path, bufio.NewReader(reader)}, nil
}

func (ir FileLineInputReader) String() string {
	return fmt.Sprintf("FileLineInputReader(%s)", ir.path)
}

func (ir FileLineInputReader) Split() ([]SingleInputReader, error) {
	return []SingleInputReader{ir}, nil
}

func (ir FileLineInputReader) Next() (interface{}, error) {
	s, err := ir.r.ReadString('\n')
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return "", err
	}

	return s[0 : len(s)-1], nil
}
