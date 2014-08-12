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
	"appengine/datastore"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
)

func reduceCompleteTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	jobKey, complete, err := parseCompleteRequest(c, pipeline, taskKey, r)
	if err != nil {
		jobFailed(c, pipeline, jobKey, fmt.Errorf("failed reduce task %s: %s\n", taskKey.Encode(), err))
		return
	} else if complete {
		return
	}

	done, job, err := taskComplete(c, jobKey, StageReducing, StageDone)
	if err != nil {
		jobFailed(c, pipeline, jobKey, fmt.Errorf("error getting task complete status: %s", err.Error()))
		return
	}

	if !done {
		c.Infof("reduce %d complete: %d jobs remaining", taskKey.IntID(), job.TasksRunning)
		return
	}

	if job.OnCompleteUrl != "" {
		successUrl := fmt.Sprintf("%s?status=%s;id=%d", job.OnCompleteUrl, TaskStatusDone, jobKey.IntID())
		pipeline.PostStatus(c, successUrl)
	}
}

func reduceTask(c appengine.Context, baseUrl string, mr MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	var writer SingleOutputWriter

	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 16384)
			bytes := runtime.Stack(stack, false)
			c.Criticalf("panic inside of reduce task %s: %s\n%s\n", taskKey.Encode(), r, stack[0:bytes])
			errMsg := fmt.Sprintf("%s", r)
			mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(errMsg)))
		}
	}()

	if task, err := getTask(c, taskKey); err != nil {
		err := fmt.Errorf("failed to get reduce task status: %s", err)
		c.Criticalf("%s", err)
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(err.Error())))
	} else if task.Status == TaskStatusRunning {
		// we think we're already running, but we got here. that means we failed
		// unexpectedly.
		errorType := "again"
		err := "restarted unexpectedly"
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=%s;error=%s", baseUrl, taskKey.Encode(), errorType, url.QueryEscape(err)))
		return
	}

	task, err := updateTask(c, taskKey, TaskStatusRunning, "", nil)
	if err != nil {
		err := fmt.Errorf("failed to update reduce task to running: %s", err)
		c.Criticalf("%s", err)
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(err.Error())))
	}

	mr.SetReduceParameters(r.FormValue("json"))

	var finalError error
	if writerName := r.FormValue("writer"); writerName == "" {
		finalError = fmt.Errorf("writer parameter required")
	} else if writer, err = mr.WriterFromName(c, writerName); err != nil {
		finalError = fmt.Errorf("error getting writer: %s", err.Error())
	} else {
		finalError = ReduceFunc(c, mr, writer, task.ReadFrom,
			makeStatusUpdateFunc(c, mr, fmt.Sprintf("%s/reducestatus", baseUrl), taskKey.Encode()))
	}

	if finalError == nil {
		if _, err := updateTask(c, taskKey, TaskStatusDone, "", writer.ToName()); err != nil {
			panic(fmt.Errorf("Could not update task: %s", err))
		}
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=done", baseUrl, taskKey.Encode()))
	} else {
		c.Errorf("reduce failed: %s", finalError)
		errorType := "error"
		if _, ok := finalError.(tryAgainError); ok {
			// wasn't fatal, go for it
			errorType = "again"
		}

		updateTask(c, taskKey, TaskStatusFailed, finalError.Error(), nil)
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=%s;error=%s", baseUrl, taskKey.Encode(), errorType, url.QueryEscape(finalError.Error())))
	}

	writer.Close(c)
}

func ReduceFunc(c appengine.Context, mr MapReducePipeline, writer SingleOutputWriter, shardNames []string,
	statusFunc StatusUpdateFunc) error {

	merger := newMerger(mr)

	toClose := make([]io.Closer, 0, len(shardNames))
	defer func() {
		for _, c := range toClose {
			c.Close()
		}
	}()

	for _, shardName := range shardNames {
		iterator, err := mr.Iterator(c, shardName, mr)
		if err != nil {
			return err
		}

		merger.addSource(iterator)
		toClose = append(toClose, iterator)
	}

	values := make([]interface{}, 1)
	var key interface{}

	if first, err := merger.next(); err != nil {
		return err
	} else if first == nil {
		c.Infof("No results to process from map")
		writer.Close(c)
		for _, shardName := range shardNames {
			if err := mr.RemoveIntermediate(c, shardName); err != nil {
				c.Errorf("failed to remove intermediate file: %s", err.Error())
			}
		}

		return nil
	} else {
		key = first.Key
		values[0] = first.Value
	}

	for !merger.empty() {
		item, err := merger.next()
		if err != nil {
			return err
		}

		if mr.Equal(key, item.Key) {
			values = append(values, item.Value)
			continue
		}

		if result, err := mr.Reduce(key, values, statusFunc); err != nil {
			if _, ok := err.(FatalError); ok {
				err = err.(FatalError).Err
			} else {
				err = tryAgainError{err}
			}
			return err
		} else if result != nil {
			if err := writer.Write(result); err != nil {
				return err
			}
		}

		key = item.Key
		values = values[0:1]
		values[0] = item.Value
	}

	if result, err := mr.Reduce(key, values, statusFunc); err != nil {
		return err
	} else if result != nil {
		if err := writer.Write(result); err != nil {
			return err
		}
	}

	if results, err := mr.ReduceComplete(statusFunc); err != nil {
		return err
	} else {
		for _, result := range results {
			if err := writer.Write(result); err != nil {
				return err
			}
		}
	}

	writer.Close(c)

	for _, shardName := range shardNames {
		if err := mr.RemoveIntermediate(c, shardName); err != nil {
			c.Errorf("failed to remove intermediate file: %s", err.Error())
		}
	}

	return nil
}
