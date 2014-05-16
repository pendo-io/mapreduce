package kyrie

import (
	"fmt"
	"io"
	"os"
)

type OutputWriter interface {
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

func NewFileLineOutputWriter(path string) (OutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return FileLineOutputWriter{}, err
	}

	return FileLineOutputWriter{path, w}, nil
}
