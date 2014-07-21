// Copyright 2014 pendo.io
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapreduce

import (
	"appengine"
	"appengine/blobstore"
	"bufio"
	"time"
)

type BlobstoreReader struct {
	Keys []appengine.BlobKey
}

func (br BlobstoreReader) ReaderNames() ([]string, error) {
	names := make([]string, len(br.Keys))
	for i := range br.Keys {
		names[i] = string(br.Keys[i])
	}

	return names, nil
}

func (br BlobstoreReader) ReaderFromName(c appengine.Context, name string) (SingleInputReader, error) {
	reader := blobstore.NewReader(c, appengine.BlobKey(name))
	return singleLineReader{bufio.NewReader(reader)}, nil
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
		return "(unnamedblob)"
	}

	return string(b.key)
}

type BlobstoreWriter struct {
	Count int
}

func (b BlobstoreWriter) WriterNames(c appengine.Context) ([]string, error) {
	result := make([]string, b.Count)
	for i := range result {
		result[i] = "(unnamedblob)"
	}

	return result, nil
}

func (m BlobstoreWriter) WriterFromName(c appengine.Context, name string) (SingleOutputWriter, error) {
	if name != "(unnamedblob)" {
		panic("bad name for blobstore writer")
	}

	w, err := blobstore.Create(appengine.Timeout(c, 15*time.Second), "text/plain")
	if err != nil {
		return nil, err
	}

	return &BlobFileLineOutputWriter{
		LineOutputWriter: LineOutputWriter{w: w},
		blobWriter:       w,
	}, nil
}

type BlobIntermediateStorage struct {
}

func (fis *BlobIntermediateStorage) CreateIntermediate(c appengine.Context, handler KeyValueHandler) (SingleIntermediateStorageWriter, error) {
	if w, err := blobstore.Create(c, "text/plain"); err != nil {
		return nil, err
	} else {
		return &BlobFileLineOutputWriter{
			LineOutputWriter: LineOutputWriter{w: w, handler: handler},
			blobWriter:       w,
		}, nil
	}
}

func (fis BlobIntermediateStorage) Store(c appengine.Context, items []MappedData, handler KeyValueHandler) (string, error) {
	w, _ := fis.CreateIntermediate(c, handler)
	for i := range items {
		if err := w.WriteMappedData(items[i]); err != nil {
			return "", err
		}
	}

	w.Close(c)

	return w.ToName(), nil
}

func (fis BlobIntermediateStorage) Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	f := blobstore.NewReader(c, appengine.BlobKey(name))

	return &ReaderIterator{bufio.NewReader(f), handler}, nil
}

func (fis BlobIntermediateStorage) RemoveIntermediate(c appengine.Context, name string) error {
	return blobstore.Delete(c, appengine.BlobKey(name))
}
