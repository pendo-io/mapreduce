package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
)

func mapCompleteTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	jobKey, err := parseCompleteRequest(c, pipeline, taskKey, r)
	if err != nil {
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
		return
	}

	mapTasks, err := gatherTasks(c, jobKey, TaskTypeMap)
	if err != nil {
		c.Errorf("error loading tasks after map complete: %s", err.Error())
		return
	}

	// we have one set for each input, each set has ReducerCount data sets in it
	// (each of which is already sorted)
	storageNames := make([][]string, len(mapTasks))

	for i := range mapTasks {
		var shardNames map[string]int
		if err = json.Unmarshal([]byte(mapTasks[i].Result), &shardNames); err != nil {
			c.Errorf("cannot unmarshal map shard names: %s", err.Error())
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
		c.Errorf("failed to allocate ids for reduce tasks: %s", err.Error())
		return
	}

	for shard := range job.WriterNames {
		shards := storageNames[shard]

		if len(shards) > 0 {
			shardSet, _ := json.Marshal(shards)
			shardParam := url.QueryEscape(string(shardSet))

			taskKey := datastore.NewKey(c, TaskEntity, "", firstId, jobKey)
			taskKeys = append(taskKeys, taskKey)
			url := fmt.Sprintf("%s/reduce?taskKey=%s;writer=%s;shards=%s",
				job.UrlPrefix, taskKey.Encode(), job.WriterNames[shard],
				shardParam)

			firstId++

			tasks = append(tasks, JobTask{
				Status:   TaskStatusPending,
				RunCount: 0,
				Url:      url,
				Type:     TaskTypeReduce,
			})
		}
	}

	if err := createTasks(c, jobKey, taskKeys, tasks, StageReducing); err != nil {
		c.Errorf("failed to create reduce tasks: %s", err.Error())
		return
	}

	for i := range tasks {
		if err := pipeline.PostTask(c, tasks[i].Url); err != nil {
			c.Errorf("failed to create post reduce task: %s", err.Error())
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
			c.Criticalf("panic inside of map task %s:\n%s\n", taskKey.Encode(), stack[0:bytes])
			errMsg := fmt.Sprintf("%s", r)
			mr.PostTask(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(errMsg)))
		}
	}()

	if readerName := r.FormValue("reader"); readerName == "" {
		finalErr = fmt.Errorf("reader parameter required")
	} else if shardStr := r.FormValue("shards"); shardStr == "" {
		finalErr = fmt.Errorf("shards parameter required")
	} else if shardCount, err := strconv.ParseInt(shardStr, 10, 32); err != nil {
		finalErr = fmt.Errorf("error parsing shard count: %s", err.Error())
	} else if reader, err := mr.ReaderFromName(readerName); err != nil {
		finalErr = fmt.Errorf("error making reader: %s", err)
	} else {
		shardNames, finalErr = mapperFunc(c, mr, reader, int(shardCount),
			makeStatusUpdateFunc(c, mr, fmt.Sprintf("%s/mapstatus", baseUrl), taskKey.Encode()))
	}

	if finalErr == nil {
		if err := updateTask(c, taskKey, TaskStatusDone, "", shardNames); err != nil {
			c.Criticalf("Could not update task: %s", err)
		}
		mr.PostTask(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=done", baseUrl, taskKey.Encode()))
	} else {
		updateTask(c, taskKey, TaskStatusFailed, finalErr.Error(), nil)
		mr.PostTask(c, fmt.Sprintf("%s/mapcomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(finalErr.Error())))
	}
}

func mapperFunc(c appengine.Context, mr MapReducePipeline, reader SingleInputReader, shardCount int,
	statusFunc StatusUpdateFunc) (map[string]int, error) {

	dataSets := make([]mappedDataList, shardCount)
	for i := range dataSets {
		dataSets[i] = mappedDataList{data: make([]MappedData, 0), compare: mr}
	}

	for item, err := reader.Next(); item != nil && err == nil; item, err = reader.Next() {
		itemList, err := mr.Map(item, statusFunc)

		if err != nil {
			return nil, err
		}

		for _, item := range itemList {
			shard := mr.Shard(item.Key, shardCount)
			dataSets[shard].data = append(dataSets[shard].data, item)
		}
	}

	names := make(map[string]int, len(dataSets))
	for i := range dataSets {
		sort.Sort(dataSets[i])
		if name, err := mr.Store(c, dataSets[i].data, mr); err != nil {
			return nil, err
		} else {
			names[name] = i
		}
	}

	return names, nil
}
