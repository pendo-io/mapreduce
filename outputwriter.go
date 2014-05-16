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

func (o FileLineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s", data)))
	return nil
}

func NewFileLineOutputWriter(path string) (OutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return FileLineOutputWriter{}, err
	}

	return FileLineOutputWriter{path, w}, nil
}
