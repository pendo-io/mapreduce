package mapreduce

import (
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
	ReaderFromName(name string) (SingleInputReader, error)
}

type SingleInputReader interface {
	Next() (interface{}, error)
}

type SingleFileLineInputReader struct {
	path string
	r    *bufio.Reader
}

type FileLineInputReader struct {
	Paths []string
}

func (m FileLineInputReader) ReaderNames() ([]string, error) {
	return m.Paths, nil
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

func (ir SingleFileLineInputReader) String() string {
	return fmt.Sprintf("SingleFileLineInputReader(%s)", ir.path)
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
