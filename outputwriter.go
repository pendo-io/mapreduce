package mapreduce

import (
	"appengine"
	"appengine/blobstore"
	"fmt"
	"io"
	"os"
)

type OutputWriter interface {
	Writers(c appengine.Context) ([]SingleOutputWriter, error)
	WriterCount() int
	WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error)
}

type FileLineOutputWriter struct {
	Paths []string
}

func (m FileLineOutputWriter) Writers(c appengine.Context) ([]SingleOutputWriter, error) {
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

type BlobFileLineOutputWriter struct {
	LineOutputWriter
	key        appengine.BlobKey
	blobWriter *blobstore.Writer
}

func (b *BlobFileLineOutputWriter) Close(c appengine.Context) {
	b.blobWriter.Close()
	b.key, _ = b.blobWriter.Key()
}

func (b *BlobFileLineOutputWriter) ToName() string {
	if string(b.key) == "" {
		return "(unnamed)"
	}

	return string(b.key)
}

type BlobstoreWriter struct {
	count int
}

func (b BlobstoreWriter) Writers(c appengine.Context) ([]SingleOutputWriter, error) {
	result := make([]SingleOutputWriter, b.count)
	for i := range result {
		result[i] = &BlobFileLineOutputWriter{}
	}

	return result, nil
}

func (b BlobstoreWriter) WriterCount() int {
	return b.count
}

func (m BlobstoreWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	if name != "(unnamed)" {
		panic("ack")
	}

	w, err := blobstore.Create(c, "text/plain")
	if err != nil {
		return nil, err
	}

	return &BlobFileLineOutputWriter{
		LineOutputWriter: LineOutputWriter{w},
		blobWriter:       w,
	}, nil
}
