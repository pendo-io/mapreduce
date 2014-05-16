package kyrie

import (
	"fmt"
	"io"
	"os"
)

type OutputWriter interface {
	Writers() ([]SingleOutputWriter, error)
	WriterCount() int
}

type FileLineOutputWriter struct {
	Paths []string
}

func (m FileLineOutputWriter) Writers() ([]SingleOutputWriter, error) {
	writers := make([]SingleOutputWriter, len(m.Paths))
	for i := range m.Paths {
		var err error
		writers[i], err = NewSingleFileLineOutputWriter(m.Paths[i])
		if err != nil {
			return nil, err
		}
	}

	return writers, nil
}

func (m FileLineOutputWriter) WriterCount() int {
	return len(m.Paths)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close()
}

type SingleFileLineOutputWriter struct {
	path string
	w    io.Writer
}

func (o SingleFileLineOutputWriter) Close() {
}

func (o SingleFileLineOutputWriter) String() string {
	return fmt.Sprintf("SingleFileLineOutputWriter(%s)", o.path)
}

func (o SingleFileLineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

func NewSingleFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return SingleFileLineOutputWriter{}, err
	}

	return SingleFileLineOutputWriter{path, w}, nil
}
