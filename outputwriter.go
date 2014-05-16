package kyrie

import (
	"fmt"
	"io"
	"os"
)

type OutputWriter interface {
	Writers() ([]SingleOutputWriter, error)
	WriterCount() int
	WriterFromName(name string) (SingleOutputWriter, error)
}

type FileLineOutputWriter struct {
	Paths []string
}

func (m FileLineOutputWriter) Writers() ([]SingleOutputWriter, error) {
	writers := make([]SingleOutputWriter, len(m.Paths))
	for i := range m.Paths {
		var err error
		writers[i], err = newSingleFileLineOutputWriter(m.Paths[i])
		if err != nil {
			return nil, err
		}
	}

	return writers, nil
}

func (m FileLineOutputWriter) WriterCount() int {
	return len(m.Paths)
}

func (m FileLineOutputWriter) WriterFromName(name string) (SingleOutputWriter, error) {
	return newSingleFileLineOutputWriter(name)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close()
	ToName() string
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

func (o SingleFileLineOutputWriter) ToName() string {
	return o.path
}

func (o SingleFileLineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

func newSingleFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return SingleFileLineOutputWriter{}, err
	}

	return SingleFileLineOutputWriter{path, w}, nil
}
