package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
)

func ReduceCompleteTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	jobKey, err := parseCompleteRequest(c, pipeline, taskKey, r)
	if err != nil {
		return
	}

	done, job, err := taskComplete(c, jobKey, StageReducing, StageDone)
	if err != nil {
		c.Errorf("error getting task complete status: %s", err.Error())
		return
	}

	if !done {
		return
	}

	successUrl := fmt.Sprintf("%s?status=%s;id=%d", job.OnCompleteUrl, TaskStatusDone, jobKey.IntID())
	pipeline.PostStatus(c, successUrl)
}

func ReduceTask(c appengine.Context, baseUrl string, mr MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	var shardNames []string
	var writer SingleOutputWriter

	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 16384)
			bytes := runtime.Stack(stack, false)
			c.Criticalf("panic inside of reduce task %s:\n%s\n", taskKey.Encode(), stack[0:bytes])
			errMsg := fmt.Sprintf("%s", r)
			mr.PostTask(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(errMsg)))
		}
	}()

	updateTask(c, taskKey, TaskStatusRunning, "", nil)

	var finalError error
	if writerName := r.FormValue("writer"); writerName == "" {
		finalError = fmt.Errorf("writer parameter required")
	} else if shardParam := r.FormValue("shards"); shardParam == "" {
		finalError = fmt.Errorf("shards parameter required")
	} else if shardJson, err := url.QueryUnescape(shardParam); err != nil {
		finalError = fmt.Errorf("cannot urldecode shards: %s", err.Error)
	} else if err := json.Unmarshal([]byte(shardJson), &shardNames); err != nil {
		fmt.Printf("json is ", shardJson)
		finalError = fmt.Errorf("cannot unmarshal shard names: %s", err.Error())
	} else if writer, err = mr.WriterFromName(c, writerName); err != nil {
		finalError = fmt.Errorf("error getting writer: %s", err.Error())
	} else {
		finalError = ReduceFunc(c, mr, writer, shardNames,
			makeStatusUpdateFunc(c, mr, fmt.Sprintf("%s/reducestatus", baseUrl), taskKey.Encode()))
	}

	if finalError == nil {
		updateTask(c, taskKey, TaskStatusDone, "", writer.ToName())
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=done", baseUrl, taskKey.Encode()))
	} else {
		updateTask(c, taskKey, TaskStatusFailed, finalError.Error(), nil)
		mr.PostStatus(c, fmt.Sprintf("%s/reducecomplete?taskKey=%s;status=error;error=%s", baseUrl, taskKey.Encode(), url.QueryEscape(finalError.Error())))
	}

	writer.Close(c)
}

func ReduceFunc(c appengine.Context, mr MapReducePipeline, writer SingleOutputWriter, shardNames []string,
	statusFunc StatusUpdateFunc) error {

	inputCount := len(shardNames)

	merger := mappedDataMerger{
		items:   make([]mappedDataMergeItem, 0, inputCount),
		compare: mr,
	}

	for _, shardName := range shardNames {
		iterator, err := mr.Iterator(c, shardName, mr)
		if err != nil {
			return err
		}

		firstItem, exists, err := iterator.Next()
		if err != nil {
			return err
		} else if !exists {
			continue
		}

		merger.items = append(merger.items, mappedDataMergeItem{iterator, firstItem})
	}

	if len(merger.items) == 0 {
		return nil
	}

	values := make([]interface{}, 1)
	var key interface{}

	if first, err := merger.next(); err != nil {
		return err
	} else {
		key = first.Key
		values[0] = first.Value
	}

	for len(merger.items) > 0 {
		item, err := merger.next()
		if err != nil {
			return err
		}

		if mr.Equal(key, item.Key) {
			values = append(values, item.Value)
			continue
		}

		if result, err := mr.Reduce(key, values, statusFunc); err != nil {
			return err
		} else if err := writer.Write(result); err != nil {
			return err
		}

		key = item.Key
		values = values[0:1]
		values[0] = item.Value
	}

	if result, err := mr.Reduce(key, values, statusFunc); err != nil {
		return err
	} else if err := writer.Write(result); err != nil {
		return err
	}

	writer.Close(c)

	for _, shardName := range shardNames {
		if err := mr.RemoveIntermediate(c, shardName); err != nil {
			c.Errorf("failed to remove intermediate file: %s", err.Error())
		}
	}

	return nil
}
