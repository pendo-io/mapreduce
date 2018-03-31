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
	"bytes"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/pendo-io/appwrap"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

func reduceMonitorTask(c context.Context, ds appwrap.Datastore, pipeline MapReducePipeline, jobKey *datastore.Key, r *http.Request, timeout time.Duration, log appwrap.Logging) int {
	start := time.Now()

	job, err := waitForStageCompletion(c, ds, pipeline, jobKey, StageReducing, StageDone, timeout, log)
	if err != nil {
		log.Criticalf("waitForStageCompletion() failed: %S", err)
		return 200
	} else if job.Stage == StageReducing {
		log.Infof("wait timed out -- returning an error and letting us automatically restart")

		return 500
	}

	log.Infof("reduce complete status: %s", job.Stage)
	if job.OnCompleteUrl != "" {
		successUrl := fmt.Sprintf("%s?status=%s;id=%d", job.OnCompleteUrl, TaskStatusDone, jobKey.IntID())
		log.Infof("posting complete status to url %s", successUrl)
		pipeline.PostStatus(c, successUrl, log)
	}

	log.Infof("reduction complete after %s of monitoring ", time.Now().Sub(start))

	return 200
}

func reduceTask(c context.Context, ds appwrap.Datastore, baseUrl string, mr MapReducePipeline, taskKey *datastore.Key, w http.ResponseWriter, r *http.Request, log appwrap.Logging) {
	var writer SingleOutputWriter
	var task JobTask
	var err error
	var retry bool

	start := time.Now()

	// we do this before starting the task below so that the parameters are set before
	// the task status callback is invoked
	mr.SetReduceParameters(r.FormValue("json"))

	if task, err, retry = startTask(c, ds, mr, taskKey, log); err != nil && retry {
		log.Criticalf("failed updating task to running: %s", err)
		http.Error(w, err.Error(), 500) // this will run us again
		return
	} else if err != nil {
		log.Criticalf("(fatal) failed updating task to running: %s", err)
		http.Error(w, err.Error(), 200) // this will run us again
		return
	}

	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 16384)
			bytes := runtime.Stack(stack, false)
			log.Criticalf("panic inside of reduce task %s: %s\n%s\n", taskKey.Encode(), r, stack[0:bytes])

			if err := retryTask(c, ds, mr, task.Job, taskKey, log); err != nil {
				panic(fmt.Errorf("failed to retry task after panic: %s", err))
			}
		}
	}()

	var finalErr error
	if writerName := r.FormValue("writer"); writerName == "" {
		finalErr = fmt.Errorf("writer parameter required")
	} else if writer, err = mr.WriterFromName(c, writerName); err != nil {
		finalErr = fmt.Errorf("error getting writer: %s", err.Error())
	} else if len(task.ReadFrom) == 0 {
		// nothing to read
	} else {
		shardReader, _ := zlib.NewReader(bytes.NewBuffer(task.ReadFrom))
		shardJson, _ := ioutil.ReadAll(shardReader)
		var shards []string
		json.Unmarshal(shardJson, &shards)

		finalErr = ReduceFunc(c, mr, writer, shards, task.SeparateReduceItems,
			makeStatusUpdateFunc(c, ds, mr, fmt.Sprintf("%s/reducestatus", baseUrl), taskKey.Encode(), log), log)
	}

	writer.Close(c)

	if err := endTask(c, ds, mr, task.Job, taskKey, finalErr, writer.ToName(), log); err != nil {
		log.Criticalf("Could not finish task: %s", err)
		http.Error(w, err.Error(), 500)
		return
	}

	log.Infof("reducer done after %s", time.Now().Sub(start))
}

func ReduceFunc(c context.Context, mr MapReducePipeline, writer SingleOutputWriter, shardNames []string,
	separateReduceItems bool, statusFunc StatusUpdateFunc, log appwrap.Logging) error {

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

	results := make([]result, len(shardNames))
	wg := sync.WaitGroup{}

	for i, shardName := range shardNames {
		i, shardName := i, shardName
		wg.Add(1)

		go func() {
			defer wg.Done()
			iterator, err := mr.Iterator(c, shardName, mr)
			results[i] = result{iterator, err}
		}()
	}

	wg.Wait()

	for i, result := range results {
		if result.err != nil {
			return tryAgainError{fmt.Errorf("cannot open intermediate file %s: %s", shardNames[i], result.err)}
		}

		merger.addSource(result.iterator)
		toClose = append(toClose, result.iterator)
	}

	values := make([]interface{}, 1)
	var key interface{}

	if first, err := merger.next(); err != nil {
		return err
	} else if first == nil {
		log.Infof("No results to process from map")
		writer.Close(c)
		for _, shardName := range shardNames {
			if err := mr.RemoveIntermediate(c, shardName); err != nil {
				log.Errorf("failed to remove intermediate file: %s", err.Error())
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
			return tryAgainError{err}
		}

		if !separateReduceItems && mr.Equal(key, item.Key) {
			values = append(values, item.Value)
			continue
		}

		if result, err := mr.Reduce(key, values, statusFunc); err != nil {
			return tryAgainIfNonFatal(err)
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
		return tryAgainIfNonFatal(err)
	} else if result != nil {
		if err := writer.Write(result); err != nil {
			return tryAgainError{err}
		}
	}

	if results, err := mr.ReduceComplete(statusFunc); err != nil {
		return err
	} else {
		for _, result := range results {
			if err := writer.Write(result); err != nil {
				return tryAgainError{err}
			}
		}
	}

	for _, shardName := range shardNames {
		if err := mr.RemoveIntermediate(c, shardName); err != nil {
			log.Errorf("failed to remove intermediate file: %s", err.Error())
		}
	}

	return nil
}
