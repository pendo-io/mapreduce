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
	"runtime"
	"time"
)

func reduceMonitorTask(c appengine.Context, pipeline MapReducePipeline, jobKey *datastore.Key, r *http.Request, timeout time.Duration) {
	start := time.Now()

	job, err := waitForStageCompletion(c, pipeline, jobKey, StageReducing, StageDone, timeout)
	if err != nil {
		c.Criticalf("waitForStageCompletion() failed: %S", err)
		return
	} else if job.Stage == StageReducing {
		c.Infof("wait timed out -- restarting monitor")
		if err := pipeline.PostStatus(c, fmt.Sprintf("%s/reduce-monitor?jobKey=%s", job.UrlPrefix, jobKey.Encode())); err != nil {
			c.Criticalf("failed to start reduce monitor task: %s", err)
		}
	}

	c.Infof("reduce complete status: %s", job.Stage)
	if job.OnCompleteUrl != "" {
		successUrl := fmt.Sprintf("%s?status=%s;id=%d", job.OnCompleteUrl, TaskStatusDone, jobKey.IntID())
		c.Infof("posting complete status to url %s", successUrl)
		pipeline.PostStatus(c, successUrl)
	}

	c.Infof("reduction complete after %s of monitoring ", time.Now().Sub(start))
}

func reduceTask(c appengine.Context, baseUrl string, mr MapReducePipeline, taskKey *datastore.Key, w http.ResponseWriter, r *http.Request) {
	var writer SingleOutputWriter
	var task JobTask
	var err error

	start := time.Now()

	// we do this before starting the task below so that the parameters are set before
	// the task status callback is invoked
	mr.SetReduceParameters(r.FormValue("json"))

	if task, err = startTask(c, mr, taskKey); err != nil {
		c.Criticalf("failed updating task to running: %s", err)
		http.Error(w, err.Error(), 500) // this will run us again
		return
	}

	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 16384)
			bytes := runtime.Stack(stack, false)
			c.Criticalf("panic inside of reduce task %s: %s\n%s\n", taskKey.Encode(), r, stack[0:bytes])

			if err := retryTask(c, mr, task.Job, taskKey); err != nil {
				panic(fmt.Errorf("failed to retry task after panic: %s", err))
			}
		}
	}()

	var finalErr error
	if writerName := r.FormValue("writer"); writerName == "" {
		finalErr = fmt.Errorf("writer parameter required")
	} else if writer, err = mr.WriterFromName(c, writerName); err != nil {
		finalErr = fmt.Errorf("error getting writer: %s", err.Error())
	} else {
		finalErr = ReduceFunc(c, mr, writer, task.ReadFrom, task.SeparateReduceItems,
			makeStatusUpdateFunc(c, mr, fmt.Sprintf("%s/reducestatus", baseUrl), taskKey.Encode()))
	}

	writer.Close(c)

	if err := endTask(c, mr, task.Job, taskKey, finalErr, writer.ToName()); err != nil {
		c.Criticalf("Could not finish task: %s", err)
		http.Error(w, err.Error(), 500)
		return
	}

	c.Infof("reducer done after %s", time.Now().Sub(start))
}

func ReduceFunc(c appengine.Context, mr MapReducePipeline, writer SingleOutputWriter, shardNames []string,
	separateReduceItems bool, statusFunc StatusUpdateFunc) error {

	merger := newMerger(mr)

	toClose := make([]io.Closer, 0, len(shardNames))
	defer func() {
		for _, c := range toClose {
			c.Close()
		}
	}()

	type result struct {
		iterator IntermediateStorageIterator
		err      error
	}

	resultsCh := make(chan result)

	for _, shardName := range shardNames {
		shardName := shardName

		go func() {
			iterator, err := mr.Iterator(c, shardName, mr)
			resultsCh <- result{iterator, err}
		}()
	}

	for _, shardName := range shardNames {
		result := <-resultsCh

		if result.err != nil {
			return fmt.Errorf("cannot open intermediate file %s: %s", shardName, result.err)
		}

		merger.addSource(result.iterator)
		toClose = append(toClose, result.iterator)
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

		if !separateReduceItems && mr.Equal(key, item.Key) {
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
