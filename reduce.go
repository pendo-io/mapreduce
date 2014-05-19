package kyrie

import (
	"appengine"
	"appengine/datastore"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func ReduceCompleteTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	var finalErr error = nil

	status := r.FormValue("status")
	switch status {
	case "":
		finalErr = fmt.Errorf("missing status for task %s", r)
	case "done":
	case "error":
		finalErr = fmt.Errorf("failed task: %s", r.FormValue("error"))
	default:
		finalErr = fmt.Errorf("unknown task status %s", status)
	}

	if finalErr != nil {
		c.Errorf("bad status from task: %s", finalErr.Error())
		return
	}

	jobKey := taskKey.Parent()
	done, job, err := taskComplete(c, jobKey, StageReducing, StageDone)
	if err != nil {
		c.Errorf("error getting task complete status: %s", err.Error())
		return
	}

	if !done {
		return
	}

	pipeline.PostTask(job.OnCompleteUrl)

}

func ReduceTask(c appengine.Context, mr MapReducePipeline, taskKey *datastore.Key, r *http.Request) {
	var err error
	var shardNames []string
	var writer SingleOutputWriter

	updateTask(c, taskKey, TaskStatusRunning, "", nil)

	if writerName := r.FormValue("writer"); writerName == "" {
		err = fmt.Errorf("writer parameter required")
	} else if shardParam := r.FormValue("shards"); shardParam == "" {
		err = fmt.Errorf("shards parameter required")
	} else if shardJson, err := url.QueryUnescape(shardParam); err != nil {
		err = fmt.Errorf("cannot urldecode shards: %s", err.Error)
	} else if err = json.Unmarshal([]byte(shardJson), &shardNames); err != nil {
		fmt.Printf("json is ", shardJson)
		err = fmt.Errorf("cannot unmarshal shard names: %s", err.Error())
	} else if writer, err = mr.WriterFromName(writerName); err != nil {
		err = fmt.Errorf("error getting writer: %s", err.Error())
	} else {
		err = ReduceFunc(mr, writer, shardNames)
	}

	if err == nil {
		updateTask(c, taskKey, TaskStatusDone, "", nil)
		mr.PostTask(fmt.Sprintf("/reducecomplete?taskKey=%s;status=done", taskKey.Encode()))
	} else {
		updateTask(c, taskKey, TaskStatusFailed, err.Error(), nil)
		mr.PostTask(fmt.Sprintf("/reducecomplete?taskKey=%s;status=error;error=%s", taskKey.Encode(), url.QueryEscape(err.Error())))
	}
}

func ReduceFunc(mr MapReducePipeline, writer SingleOutputWriter, shardNames []string) error {
	inputCount := len(shardNames)

	items := shardMappedDataList{
		feeders: make([]IntermediateStorageIterator, 0, inputCount),
		data:    make([]MappedData, 0, inputCount),
		compare: mr,
	}

	for _, shardName := range shardNames {
		iterator, err := mr.Iterator(shardName, mr)
		if err != nil {
			return err
		}

		firstItem, exists, err := iterator.Next()
		if err != nil {
			return err
		} else if !exists {
			continue
		}

		items.feeders = append(items.feeders, iterator)
		items.data = append(items.data, firstItem)
	}

	if len(items.data) == 0 {
		return nil
	}

	values := make([]interface{}, 1)
	var key interface{}

	if first, err := items.next(); err != nil {
		return err
	} else {
		key = first.Key
		values[0] = first.Value
	}

	for len(items.data) > 0 {
		item, err := items.next()
		if err != nil {
			return err
		}

		if mr.Equal(key, item.Key) {
			values = append(values, item.Value)
			continue
		}

		if result, err := mr.Reduce(key, values); err != nil {
			return err
		} else if err := writer.Write(result); err != nil {
			return err
		}

		key = item.Key
		values = values[0:1]
		values[0] = item.Value
	}

	if result, err := mr.Reduce(key, values); err != nil {
		return err
	} else if err := writer.Write(result); err != nil {
		return err
	}

	writer.Close()

	return nil
}
