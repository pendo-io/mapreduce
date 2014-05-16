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

type MultiOutputWriter struct {
	writers []SingleOutputWriter
}

func (m MultiOutputWriter) Writers() ([]SingleOutputWriter, error) {
	return m.writers, nil
}

func (m MultiOutputWriter) WriterCount() int {
	return len(m.writers)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close()
}

type FileLineOutputWriter struct {
	path string
	w    io.Writer
}

func (o FileLineOutputWriter) Close() {
}

func (o FileLineOutputWriter) String() string {
	return fmt.Sprintf("FileLineOutputWriter(%s)", o.path)
}

func (o FileLineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

func NewFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return FileLineOutputWriter{}, err
	}

	return FileLineOutputWriter{path, w}, nil
}
