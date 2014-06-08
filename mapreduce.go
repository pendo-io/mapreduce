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

// Package mapreduce provides a mapreduce pipeline for Google's appengine environment
package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"container/heap"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// MappedData items are key/value pairs returned from the Map stage. The items are rearranged
// by the shuffle, and (Key, []Value) pairs are passed into the shuffle. KeyHandler interfaces
// provide the operations on MappedData items which are needed by the pipeline, and ValueHandler
// interfaces provide serialization operatons for the values.
type MappedData struct {
	Key   interface{}
	Value interface{}
}

// StatusUpdateFunc functions are passed into Map and Reduce handlers to allow those handlers
// to post arbitrary status messages which are stored in the datastore
type StatusUpdateFunc func(format string, paramList ...interface{})

// Mapper defines a map function; it is passed an item from the input and returns
// a list of mapped items.
type Mapper interface {
	Map(item interface{}, statusUpdate StatusUpdateFunc) ([]MappedData, error)

	// Called once with the job parameters for each mapper task
	SetMapParameters(jsonParameters string)
}

// Reducer defines the reduce function; it is called once for each key and is given a list
// of all of the values for that key.
type Reducer interface {
	Reduce(key interface{}, values []interface{}, statusUpdate StatusUpdateFunc) (result interface{}, err error)

	// Called once with the job parameters for each mapper task
	SetReduceParameters(jsonParameters string)

	// Called when the reduce is complete. Each item in the results array will be passed separately
	// to the output writer
	ReduceComplete(statusUpdate StatusUpdateFunc) ([]interface{}, error)
}

// FatalError wraps an error. If Map or Reduce returns a FatalError the task will not be retried
type FatalError struct{ Err error }

func (fe FatalError) Error() string { return fe.Err.Error() }

// tryAgainError is the inverse of a fatal error; we rework Map() and Reduce() in terms of tryAgainError because
// it makes our internal errors not wrapped at all, making life simpler
type tryAgainError struct{ err error }

func (tae tryAgainError) Error() string { return tae.err.Error() }

// MapReducePipeline defines the complete pipeline for a map reduce job (but not the job itself).
// No per-job information is available for the pipeline functions other than what gets passed in
// via the various interfaces.
type MapReducePipeline interface {
	// The basic pipeline of read, map, shuffle, reduce, save
	InputReader
	Mapper
	IntermediateStorage
	Reducer
	OutputWriter

	// Serialization and sorting primatives for keys and values
	KeyHandler
	ValueHandler

	TaskInterface
}

// MapReduceJob defines a complete map reduce job, which is the pipeline and the parameters the job
// needs. The types for Inputs and Outputs must match the types for the InputReader and OutputWriter
// in the pipeline.
type MapReduceJob struct {
	MapReducePipeline
	Inputs  InputReader
	Outputs OutputWriter

	// UrlPrefix is the base url path used for mapreduce jobs posted into
	// task queues, and must match the baseUrl passed into MapReduceHandler()
	UrlPrefix string

	// OnCompleteUrl is the url to post to when a job is completed. The full url will include
	// multiple query parameters, including status=(done|error) and id=(jobId). If
	// an error occurred the error parameter will also be displayed. If this is empty, no
	// complete notification is given; it is assumed the caller will poll for results.
	OnCompleteUrl string

	// RetryCount is the number of times individual map/reduce tasks should be retried. Tasks that
	// return errors which are of type FatalError are not retried (defaults to 3, 1
	// means it will never retry).
	RetryCount int

	// JobParameters is passed to map and reduce job. They are assumed to be json encoded, though
	// absolutely no effort is made to enforce that.
	JobParameters string
}

type mappedDataList struct {
	data    []MappedData
	compare KeyHandler
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

type mappedDataMergeItem struct {
	iterator IntermediateStorageIterator
	datum    MappedData
}

type mappedDataMerger struct {
	items   []mappedDataMergeItem
	compare KeyHandler
	inited  bool
}

func (a *mappedDataMerger) Len() int { return len(a.items) }
func (a *mappedDataMerger) Less(i, j int) bool {
	return a.compare.Less(a.items[i].datum.Key, a.items[j].datum.Key)
}
func (a *mappedDataMerger) Swap(i, j int)      { a.items[i], a.items[j] = a.items[j], a.items[i] }
func (a *mappedDataMerger) Push(x interface{}) { a.items = append(a.items, x.(mappedDataMergeItem)) }
func (a *mappedDataMerger) Pop() interface{} {
	x := a.items[len(a.items)-1]
	a.items = a.items[0 : len(a.items)-1]
	return x
}

func (s *mappedDataMerger) next() (MappedData, error) {
	if !s.inited {
		heap.Init(s)
		s.inited = true
	}

	item := heap.Pop(s).(mappedDataMergeItem)

	if newItem, exists, err := item.iterator.Next(); err != nil {
		return MappedData{}, err
	} else if exists {
		heap.Push(s, mappedDataMergeItem{item.iterator, newItem})
	}

	return item.datum, nil
}

func Run(c appengine.Context, job MapReduceJob) (int64, error) {
	readerNames, err := job.Inputs.ReaderNames()
	if err != nil {
		return 0, err
	} else if len(readerNames) == 0 {
		return 0, fmt.Errorf("no input readers")
	}

	writerNames, err := job.Outputs.WriterNames(c)
	if err != nil {
		return 0, err
	} else if len(writerNames) == 0 {
		return 0, fmt.Errorf("no output writers")
	}

	reducerCount := len(writerNames)

	jobKey, err := createJob(c, job.UrlPrefix, writerNames, job.OnCompleteUrl, job.JobParameters, job.RetryCount)
	if err != nil {
		return 0, err
	}

	taskKeys := make([]*datastore.Key, len(readerNames))
	tasks := make([]JobTask, len(readerNames))
	firstId, _, err := datastore.AllocateIDs(c, TaskEntity, jobKey, len(readerNames))
	if err != nil {
		return 0, err
	}

	for i, readerName := range readerNames {
		taskKeys[i] = datastore.NewKey(c, TaskEntity, "", firstId, jobKey)
		firstId++

		url := fmt.Sprintf("%s/map?taskKey=%s;reader=%s;shards=%d",
			job.UrlPrefix, taskKeys[i].Encode(), readerName,
			reducerCount)

		tasks[i] = JobTask{
			Status:   TaskStatusPending,
			RunCount: 0,
			Url:      url,
			Type:     TaskTypeMap,
		}
	}

	if err := createTasks(c, jobKey, taskKeys, tasks, StageMapping); err != nil {
		return 0, err
	}

	for i := range tasks {
		if err := job.PostTask(c, tasks[i].Url, job.JobParameters); err != nil {
			return 0, err
		}
	}

	return jobKey.IntID(), nil
}

type urlHandler struct {
	pipeline   MapReducePipeline
	baseUrl    string
	getContext func(r *http.Request) appengine.Context
}

// MapReduceHandler returns an http.Handler which is responsible for all of the
// urls pertaining to the mapreduce job. The baseUrl acts as the name for the
// type of job being run.
func MapReduceHandler(baseUrl string, pipeline MapReducePipeline,
	getContext func(r *http.Request) appengine.Context) http.Handler {

	return urlHandler{pipeline, baseUrl, getContext}
}

func (h urlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var taskKey *datastore.Key
	var err error

	if taskKeyStr := r.FormValue("taskKey"); taskKeyStr == "" {
		http.Error(w, "taskKey parameter required", http.StatusBadRequest)
		return
	} else if taskKey, err = datastore.DecodeKey(taskKeyStr); err != nil {
		http.Error(w, fmt.Sprintf("invalid taskKey: %s", err.Error()),
			http.StatusBadRequest)
		return
	}

	c := h.getContext(r)

	if strings.HasSuffix(r.URL.Path, "/reduce") {
		reduceTask(c, h.baseUrl, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/reducecomplete") {
		reduceCompleteTask(c, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/map") {
		mapTask(c, h.baseUrl, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/mapcomplete") {
		mapCompleteTask(c, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/mapstatus") ||
		strings.HasSuffix(r.URL.Path, "/reducestatus") {

		updateTask(c, taskKey, "", r.FormValue("msg"), nil)
	} else {
		http.Error(w, "unknown request url", http.StatusNotFound)
		return
	}
}

func makeStatusUpdateFunc(c appengine.Context, pipeline MapReducePipeline, urlStr string, taskKey string) StatusUpdateFunc {
	return func(format string, paramList ...interface{}) {
		pipeline.PostStatus(c, fmt.Sprintf("%s?taskKey=%s;msg=%s", urlStr, taskKey,
			url.QueryEscape(fmt.Sprintf(format, paramList...))))
	}
}
