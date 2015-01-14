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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
)

func mapCompleteTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	jobKey, complete, err := parseCompleteRequest(c, pipeline, taskKey, r)
	if err != nil {
		c.Errorf("failed map task %s: %s\n", taskKey.Encode(), err)
		return
	} else if complete {
		return
	}

	done, job, err := taskComplete(c, jobKey, StageMapping, StageReducing)
	if err != nil {
		c.Errorf("error getting map task complete status: %s", err.Error())
		return
	}

	// we'll never get here if the task was marked failed because of taskComplete's check
	// of the current stage

	if !done {
		c.Infof("map %d complete: %d jobs remaining", taskKey.IntID(), job.TasksRunning)
		return
	}

	mapTasks, err := gatherTasks(c, jobKey, TaskTypeMap)
	if err != nil {
		jobFailed(c, pipeline, jobKey, fmt.Errorf("error loading tasks after map complete: %s", err.Error()))
		return
	}

	// we have one set for each reducer task
	storageNames := make([][]string, len(job.WriterNames))

	for i := range mapTasks {
		var shardNames map[string]int
		if err = json.Unmarshal([]byte(mapTasks[i].Result), &shardNames); err != nil {
			c.Errorf("unmarshall error for result i=%d '%s'", i, mapTasks[i].Result)
			jobFailed(c, pipeline, jobKey, fmt.Errorf("cannot unmarshal map shard names: %s", err.Error()))
			return
		} else {
			for name, shard := range shardNames {
				storageNames[shard] = append(storageNames[shard], name)
			}
		}
	}

	tasks := make([]JobTask, 0, len(job.WriterNames))
	taskKeys := make([]*datastore.Key, 0, len(job.WriterNames))
	firstId, _, err := datastore.AllocateIDs(c, TaskEntity, jobKey, len(job.WriterNames))
	if err != nil {
		jobFailed(c, pipeline, jobKey, fmt.Errorf("failed to allocate ids for reduce tasks: %s", err.Error()))
		return
	}

	for shard := range job.WriterNames {
		shards := storageNames[shard]

		if len(shards) > 0 {
			taskKey := datastore.NewKey(c, TaskEntity, "", firstId, jobKey)
			taskKeys = append(taskKeys, taskKey)
			url := fmt.Sprintf("%s/reduce?taskKey=%s;shard=%d;writer=%s",
				job.UrlPrefix, taskKey.Encode(), shard, url.QueryEscape(job.WriterNames[shard]))

			firstId++

			tasks = append(tasks, JobTask{
				Status:              TaskStatusPending,
				RunCount:            0,
				Url:                 url,
				ReadFrom:            shards,
				SeparateReduceItems: job.SeparateReduceItems,
				Type:                TaskTypeReduce,
			})
		}
	}

	if err := createTasks(c, jobKey, taskKeys, tasks, StageReducing); err != nil {
		jobFailed(c, pipeline, jobKey, fmt.Errorf("failed to create reduce tasks: %s", err.Error()))
		return
	}

	for i := range tasks {
		if err := pipeline.PostTask(c, tasks[i].Url, job.JsonParameters); err != nil {
			jobFailed(c, pipeline, jobKey, fmt.Errorf("failed to post reduce task: %s", err.Error()))
			return
		}
	}
}

func mapTask(c appengine.Context, baseUrl string, mr MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	var finalErr error
	var shardNames map[string]int

	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 16384)
			bytes := runtime.Stack(stack, false)
			c.Criticalf("panic inside of map task %s: %s\n%s\n", taskKey.Encode(), r, stack[0:bytes])
			errMsg := fmt.Sprintf("%s", r)
			mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(errMsg)))
		}
	}()

	if task, err := getTask(c, taskKey); err != nil {
		err := fmt.Errorf("failed to get map task status: %s", err)
		c.Criticalf("%s", err)
		mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(err.Error())))
	} else if task.Status == TaskStatusRunning {
		// we think we're already running, but we got here. that means we failed
		// unexpectedly.
		errorType := "again"
		err := "restarted unexpectedly"
		if _, err := updateTask(c, taskKey, TaskStatusPending, "", ""); err != nil {
			c.Errorf("failed to reset task to running status: %s", err)
		}
		mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=%s;error=%s", baseUrl, taskKey.Encode(), errorType, url.QueryEscape(err)))
		return
	}

	_, err := updateTask(c, taskKey, TaskStatusRunning, "", nil)
	if err != nil {
		err := fmt.Errorf("failed to update map task to running: %s", err)
		c.Criticalf("%s", err)
		mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(err.Error())))
		return
	}

	jsonParameters := r.FormValue("json")
	mr.SetMapParameters(jsonParameters)
	mr.SetShardParameters(jsonParameters)

	if readerName := r.FormValue("reader"); readerName == "" {
		finalErr = fmt.Errorf("reader parameter required")
	} else if shardStr := r.FormValue("shards"); shardStr == "" {
		finalErr = fmt.Errorf("shards parameter required")
	} else if shardCount, err := strconv.ParseInt(shardStr, 10, 32); err != nil {
		finalErr = fmt.Errorf("error parsing shard count: %s", err.Error())
	} else if reader, err := mr.ReaderFromName(c, readerName); err != nil {
		finalErr = fmt.Errorf("error making reader: %s", err)
	} else {
		shardNames, finalErr = mapperFunc(c, mr, reader, int(shardCount),
			makeStatusUpdateFunc(c, mr, fmt.Sprintf("%s/mapstatus", baseUrl), taskKey.Encode()))
	}

	if finalErr == nil {
		if _, err := updateTask(c, taskKey, TaskStatusDone, "", shardNames); err != nil {
			panic(fmt.Errorf("Could not update task: %s", err))
		}
		mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=done", baseUrl, taskKey.Encode()))
	} else {
		errorType := "error"
		if _, ok := finalErr.(tryAgainError); ok {
			// wasn't fatal, go for it
			errorType = "again"
		}

		updateTask(c, taskKey, TaskStatusFailed, finalErr.Error(), nil)
		mr.PostStatus(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=%s;error=%s", baseUrl, taskKey.Encode(), errorType, url.QueryEscape(finalErr.Error())))
	}
}

func mapperFunc(c appengine.Context, mr MapReducePipeline, reader SingleInputReader, shardCount int,
	statusFunc StatusUpdateFunc) (map[string]int, error) {

	dataSets := make([]mappedDataList, shardCount)
	spills := make([]spillStruct, 0)
	for i := range dataSets {
		dataSets[i] = mappedDataList{data: make([]MappedData, 0), compare: mr}
	}

	var err error
	var item interface{}
	size := 0
	count := 0
	for item, err = reader.Next(); item != nil && err == nil; item, err = reader.Next() {
		itemList, err := mr.Map(item, statusFunc)

		if err != nil {
			if _, ok := err.(FatalError); ok {
				err = err.(FatalError).Err
			} else {
				err = tryAgainError{err}
			}

			return nil, err
		}

		for _, mappedItem := range itemList {
			shard := mr.Shard(mappedItem.Key, shardCount)
			dataSets[shard].data = append(dataSets[shard].data, mappedItem)

			val, _ := mr.ValueDump(mappedItem.Value)
			size += len(val)
			count++
		}

		if size > 4*1024*1024 {
			if spill, err := writeSpill(c, mr, dataSets); err != nil {
				return nil, tryAgainError{err}
			} else {
				spills = append(spills, spill)
			}

			c.Infof("wrote spill of %d items", count)

			size = 0
			count = 0
			for shard := range dataSets {
				dataSets[shard].data = dataSets[shard].data[0:0]
			}
		}
	}

	reader.Close()

	if err != nil {
		return nil, err
	}

	itemList, err := mr.MapComplete(statusFunc)
	if err != nil {
		if _, ok := err.(FatalError); ok {
			err = err.(FatalError).Err
		} else {
			err = tryAgainError{err}
		}

		return nil, err
	}

	for _, item := range itemList {
		shard := mr.Shard(item.Key, shardCount)
		dataSets[shard].data = append(dataSets[shard].data, item)
	}

	if spill, err := writeSpill(c, mr, dataSets); err != nil {
		return nil, tryAgainError{err}
	} else {
		spills = append(spills, spill)
	}

	finalNames := make(map[string]int)
	for try := 0; try < 5; try++ {
		if names, err := mergeSpills(c, mr, mr, spills); err != nil {
			c.Infof("spill merge failed try %d: %s", try, err)
		} else {
			for shard, name := range names {
				finalNames[name] = shard
			}
			break
		}
	}

	if err != nil {
		return nil, tryAgainError{err}
	}

	c.Infof("finalNames: %#v", finalNames)

	return finalNames, nil
}
