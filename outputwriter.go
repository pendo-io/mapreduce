package mapreduce

import (
	"appengine"
	"fmt"
	"io"
	"os"
)

type OutputWriter interface {
	WriterNames(c appengine.Context) ([]string, error)
	WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error)
}

type FileLineOutputWriter struct {
	Paths []string
}

func (m FileLineOutputWriter) WriterNames(c appengine.Context) ([]string, error) {
	return m.Paths, nil
}

func (m FileLineOutputWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	return newSingleFileLineOutputWriter(name)
}

type SingleOutputWriter interface {
	Write(data interface{}) error
	Close(c appengine.Context)
	ToName() string
}

type LineOutputWriter struct {
	w io.Writer
}

func (o LineOutputWriter) Write(data interface{}) error {
	o.w.Write([]byte(fmt.Sprintf("%s\n", data)))
	return nil
}

type SingleFileLineOutputWriter struct {
	LineOutputWriter
	path string
}

func (o LineOutputWriter) Close(c appengine.Context) {
}

func (o SingleFileLineOutputWriter) ToName() string {
	return o.path
}

func newSingleFileLineOutputWriter(path string) (SingleOutputWriter, error) {
	w, err := os.Create(path)
	if err != nil {
		return SingleFileLineOutputWriter{}, err
	}

	return SingleFileLineOutputWriter{path: path, LineOutputWriter: LineOutputWriter{w}}, nil
}

type NilOutputWriter struct {
	count int
}

func (n NilOutputWriter) WriterNames(c appengine.Context) ([]string, error) {
	result := make([]string, n.count)
	for i := range result {
		result[i] = "(niloutputwriter)"
	}

	return result, nil
}

func (n NilOutputWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	return NilSingleOutputWriter{}, nil
}

type NilSingleOutputWriter struct{}

func (n NilSingleOutputWriter) Write(data interface{}) error {
	return nil
}

func (n NilSingleOutputWriter) Close(c appengine.Context) {
}

func (n NilSingleOutputWriter) ToName() string {
	return "(niloutput)"
}
