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
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/pendo-io/appwrap"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
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

	// Called when the map is complete. Return is same as for Map()
	// to the output writer
	MapComplete(statusUpdate StatusUpdateFunc) ([]MappedData, error)
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

// TaskStatusChange allows the map reduce framework to notify tasks when their status has changed to RUNNING or DONE. Handy for
// callbacks. Always called after SetMapParameters() and SetReduceParameters()
type TaskStatusChange interface {
	Status(jobId int64, task JobTask)
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
	TaskStatusChange
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

	// SeparateReduceItems means that instead of collapsing all rows with the same key into
	// one call to the reduce function, each row is passed individually (though wrapped in
	// an array of length one to keep the reduce function signature the same)
	SeparateReduceItems bool

	// JobParameters is passed to map and reduce job. They are assumed to be json encoded, though
	// absolutely no effort is made to enforce that.
	JobParameters string
}

func Run(c context.Context, ds appwrap.Datastore, job MapReduceJob) (int64, error) {
	log := appwrap.NewAppengineLogging(c)

	readerNames, err := job.Inputs.ReaderNames()
	if err != nil {
		return 0, fmt.Errorf("forming reader names: %s", err)
	} else if len(readerNames) == 0 {
		return 0, fmt.Errorf("no input readers")
	}

	writerNames, err := job.Outputs.WriterNames(c)
	if err != nil {
		return 0, fmt.Errorf("forming writer names: %s", err)
	} else if len(writerNames) == 0 {
		return 0, fmt.Errorf("no output writers")
	}

	reducerCount := len(writerNames)

	jobKey, err := createJob(ds, job.UrlPrefix, writerNames, job.OnCompleteUrl, job.SeparateReduceItems, job.JobParameters, job.RetryCount)
	if err != nil {
		return 0, fmt.Errorf("creating job: %s", err)
	}

	firstId, err := mkIds(ds, TaskEntity, len(readerNames))
	if err != nil {
		return 0, fmt.Errorf("allocating keys: %s", err)
	}

	taskKeys := makeTaskKeys(ds, firstId, len(readerNames))
	tasks := make([]JobTask, len(readerNames))

	for i, readerName := range readerNames {
		url := fmt.Sprintf("%s/map?taskKey=%s;reader=%s;shards=%d",
			job.UrlPrefix, taskKeys[i].Encode(), readerName,
			reducerCount)

		tasks[i] = JobTask{
			Status: TaskStatusPending,
			Url:    url,
			Type:   TaskTypeMap,
		}
	}

	if err := createTasks(ds, jobKey, taskKeys, tasks, StageMapping, log); err != nil {
		if _, innerErr := markJobFailed(c, ds, jobKey, log); err != nil {
			log.Errorf("failed to log job %d as failed: %s", appwrap.KeyIntID(jobKey), innerErr)
		}
		return 0, fmt.Errorf("creating tasks: %s", err)
	}

	for i := range tasks {
		if err := job.PostTask(c, tasks[i].Url, job.JobParameters, log); err != nil {
			if _, innerErr := markJobFailed(c, ds, jobKey, log); err != nil {
				log.Errorf("failed to log job %d as failed: %s", appwrap.KeyIntID(jobKey), innerErr)
			}
			return 0, fmt.Errorf("posting task: %s", err)
		}
	}

	if err := job.PostStatus(c, fmt.Sprintf("%s/map-monitor?jobKey=%s", job.UrlPrefix, jobKey.Encode()), log); err != nil {
		log.Criticalf("failed to start map monitor task: %s", err)
	}

	return appwrap.KeyIntID(jobKey), nil
}

type urlHandler struct {
	pipeline   MapReducePipeline
	baseUrl    string
	getContext func(r *http.Request) context.Context
}

// MapReduceHandler returns an http.Handler which is responsible for all of the
// urls pertaining to the mapreduce job. The baseUrl acts as the name for the
// type of job being run.
func MapReduceHandler(baseUrl string, pipeline MapReducePipeline,
	getContext func(r *http.Request) context.Context) http.Handler {

	return urlHandler{pipeline, baseUrl, getContext}
}

func (h urlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := h.getContext(r)
	ds, err := appwrap.NewDatastore(c)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	log := appwrap.NewAppengineLogging(c)

	monitorTimeout := time.Minute * 30
	if appengine.IsDevAppServer() {
		monitorTimeout = time.Second * 10
	}

	if strings.HasSuffix(r.URL.Path, "/map-monitor") || strings.HasSuffix(r.URL.Path, "/reduce-monitor") {
		if jobKeyStr := r.FormValue("jobKey"); jobKeyStr == "" {
			http.Error(w, "jobKey parameter required", http.StatusBadRequest)
		} else if jobKey, err := appwrap.DecodeKey(jobKeyStr); err != nil {
			http.Error(w, fmt.Sprintf("invalid jobKey: %s", err.Error()),
				http.StatusBadRequest)
		} else if strings.HasSuffix(r.URL.Path, "/map-monitor") {
			w.WriteHeader(mapMonitorTask(c, ds, h.pipeline, jobKey, r, monitorTimeout, log))
		} else {
			w.WriteHeader(reduceMonitorTask(c, ds, h.pipeline, jobKey, r, monitorTimeout, log))
		}

		return
	}

	var taskKey *appwrap.DatastoreKey

	if taskKeyStr := r.FormValue("taskKey"); taskKeyStr == "" {
		http.Error(w, "taskKey parameter required", http.StatusBadRequest)
		return
	} else if taskKey, err = appwrap.DecodeKey(taskKeyStr); err != nil {
		http.Error(w, fmt.Sprintf("invalid taskKey: %s", err.Error()),
			http.StatusBadRequest)
		return
	}

	log = appwrap.PrefixLogger{Logging: log, Prefix: fmt.Sprintf("%s: ", taskKey)}

	if strings.HasSuffix(r.URL.Path, "/reduce") {
		reduceTask(c, ds, h.baseUrl, h.pipeline, taskKey, w, r, log)
	} else if strings.HasSuffix(r.URL.Path, "/map") {
		mapTask(c, ds, h.baseUrl, h.pipeline, taskKey, w, r, log)
	} else if strings.HasSuffix(r.URL.Path, "/mapstatus") ||
		strings.HasSuffix(r.URL.Path, "/reducestatus") {

		updateTask(ds, taskKey, "", 0, r.FormValue("msg"), nil)
	} else {
		http.Error(w, "unknown request url", http.StatusNotFound)
		return
	}
}

func makeStatusUpdateFunc(c context.Context, ds appwrap.Datastore, pipeline MapReducePipeline, urlStr string, taskKey string, log appwrap.Logging) StatusUpdateFunc {
	return func(format string, paramList ...interface{}) {
		msg := fmt.Sprintf(format, paramList...)
		if key, err := appwrap.DecodeKey(taskKey); err != nil {
			log.Errorf("failed to decode task key for status: %s", err)
		} else if _, err := updateTask(ds, key, "", 0, msg, nil); err != nil {
			log.Errorf("failed to update task status: %s", err)
		}
	}
}

// IgnoreTaskStatusChange is an implementation of TaskStatusChange which ignores the call
type IgnoreTaskStatusChange struct{}

func (e *IgnoreTaskStatusChange) Status(jobId int64, task JobTask) {
}

// tryAgainIfNonFatal will take a non-nil error and wrap it in a tryAgainError
// if it doesn't match a FatalError.
func tryAgainIfNonFatal(err error) error {
	if err != nil {
		if _, ok := err.(FatalError); ok {
			err = err.(FatalError).Err
		} else {
			err = tryAgainError{err}
		}
		return err
	}
	return nil
}

func mkIds(ds appwrap.Datastore, kind string, count int) (int64, error) {
	incomplete := make([]*appwrap.DatastoreKey, count)
	for i := range incomplete {
		incomplete[i] = ds.NewKey(kind, "", 0, nil)
	}

	if completeKeys, err := ds.AllocateIDSet(incomplete); err != nil {
		return 0, fmt.Errorf("reserving keys: %s", err)
	} else {
		ids := make([]int, len(completeKeys))
		for i, k := range completeKeys {
			ids[i] = int(appwrap.KeyIntID(k))
		}

		sort.Sort(sort.IntSlice(ids))

		for i := 0; i < len(ids); i++ {
			if ids[i] != ids[i+1]-1 {
				return 0, fmt.Errorf("nonconsecutive keys allocated")
			}
		}

		return int64(ids[0]), nil
	}
}
