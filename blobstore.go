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
)

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

type BlobIntermediateStorage struct {
}

func (fis BlobIntermediateStorage) Store(c appengine.Context, items []MappedData, handler KeyValueHandler) (string, error) {

	if writer, err := blobstore.Create(c, "text/plain"); err != nil {
		return "", err
	} else if err := copyItemsToWriter(items, handler, writer); err != nil {
		return "", err
	} else if err := writer.Close(); err != nil {
		return "", err
	} else if key, err := writer.Key(); err != nil {
		return "", err
	} else {
		return string(key), nil
	}
}

func (fis BlobIntermediateStorage) Iterator(c appengine.Context, name string, handler KeyValueHandler) (IntermediateStorageIterator, error) {
	f := blobstore.NewReader(c, appengine.BlobKey(name))

	return &ReaderIterator{bufio.NewReader(f), handler}, nil
}

func (fis BlobIntermediateStorage) RemoveIntermediate(c appengine.Context, name string) error {
	return blobstore.Delete(c, appengine.BlobKey(name))
}
