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
	"appengine/taskqueue"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

type TaskStatus string

const (
	TaskStatusPending = TaskStatus("pending")
	TaskStatusRunning = TaskStatus("running")
	TaskStatusDone    = TaskStatus("done")
	TaskStatusFailed  = TaskStatus("failed")
)

type JobStage string

const (
	StageFormation = JobStage("forming")
	StageMapping   = JobStage("map")
	StageReducing  = JobStage("reduce")
	StageDone      = JobStage("done")
	StageFailed    = JobStage("failed")
)

// JobTask is the entity stored in the datastore defining a single MapReduce task. They
// have JobInfo entities as their parents.
type JobTask struct {
	Status    TaskStatus
	RunCount  int
	Info      string
	StartTime time.Time
	UpdatedAt time.Time
	Type      TaskType
	Retries   int
	// this is named intermediate storage sources, and only used for reduce tasks
	ReadFrom []string `datastore:",noindex"`
	Url      string   `datastore:",noindex"`
	Result   string   `datastore:",noindex"`
}

// JobInfo is the entity stored in the datastore defining the MapReduce Job
type JobInfo struct {
	UrlPrefix      string
	Stage          JobStage
	UpdatedAt      time.Time
	TasksRunning   int
	RetryCount     int
	OnCompleteUrl  string
	WriterNames    []string `datastore:",noindex"`
	JsonParameters string   `datastore:",noindex"`
}

// TaskInterface defines how the map and reduce tasks and controlled, and how they report
// their status.
type TaskInterface interface {
	PostTask(c appengine.Context, fullUrl string, jsonParameters string) error
	PostStatus(c appengine.Context, fullUrl string) error
}

type TaskType string

// TaskTypes defines the type of task, map or reduce
const (
	TaskTypeMap    = TaskType("map")
	TaskTypeReduce = TaskType("reduce")
)

// Datastore entity kinds for jobs and tasks
const JobEntity = "MapReduceJob"
const TaskEntity = "MapReduceTask"

func createJob(c appengine.Context, urlPrefix string, writerNames []string, onCompleteUrl string, jsonParameters string, retryCount int) (*datastore.Key, error) {
	if retryCount == 0 {
		// default
		retryCount = 3
	}

	key := datastore.NewKey(c, JobEntity, "", 0, nil)
	job := JobInfo{
		UrlPrefix:      urlPrefix,
		Stage:          StageFormation,
		UpdatedAt:      time.Now(),
		OnCompleteUrl:  onCompleteUrl,
		WriterNames:    writerNames,
		RetryCount:     retryCount,
		JsonParameters: jsonParameters,
	}

	return datastore.Put(c, key, &job)
}

func createTasks(c appengine.Context, jobKey *datastore.Key, taskKeys []*datastore.Key, tasks []JobTask, newStage JobStage) error {
	now := time.Now()
	for i := range tasks {
		tasks[i].StartTime = now
	}

	err := datastore.RunInTransaction(c, func(c appengine.Context) error {
		var job JobInfo

		if err := datastore.Get(c, jobKey, &job); err != nil {
			return err
		}

		job.TasksRunning = len(tasks)
		job.Stage = newStage

		if _, err := datastore.Put(c, jobKey, &job); err != nil {
			return err
		}

		_, err := datastore.PutMulti(c, taskKeys, tasks)
		return err
	}, nil)

	return err
}

func runInTransaction(c appengine.Context, tryCount int, f func(c appengine.Context) error) error {
	var finalErr error
	for i := 0; i < tryCount; i++ {
		finalErr = datastore.RunInTransaction(c, f, nil)

		if finalErr == nil {
			return nil
		} else if finalErr != nil && finalErr != datastore.ErrConcurrentTransaction {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return finalErr
}

func updateJobStage(c appengine.Context, jobKey *datastore.Key, status JobStage) (prev JobInfo, finalErr error) {
	finalErr = runInTransaction(c, 5, func(c appengine.Context) error {
		prev = JobInfo{}
		if err := datastore.Get(c, jobKey, &prev); err != nil {
			return err
		}

		job := prev
		job.Stage = status

		_, err := datastore.Put(c, jobKey, &job)
		return err
	})

	if finalErr != nil {
		c.Criticalf("updateJobStage failed: %s", finalErr)
	}

	return
}

func taskComplete(c appengine.Context, jobKey *datastore.Key, expectedStage, nextStage JobStage) (stageChanged bool, job JobInfo, finalErr error) {
	finalErr = runInTransaction(c, 5, func(c appengine.Context) error {
		job = JobInfo{}
		if err := datastore.Get(c, jobKey, &job); err != nil {
			return err
		}

		if job.Stage != expectedStage {
			stageChanged = false
			return nil
		} else if job.TasksRunning > 1 {
			job.TasksRunning--
			_, err := datastore.Put(c, jobKey, &job)
			return err
		}

		if job.TasksRunning == 0 {
			// this shouldn't really happen
			c.Errorf("job in stage %s has zero tasks remaining", job.Stage)
		} else {
			job.TasksRunning--
		}

		job.Stage = nextStage
		job.UpdatedAt = time.Now()
		_, err := datastore.Put(c, jobKey, &job)
		stageChanged = err == nil
		return err
	})

	if finalErr != nil {
		c.Criticalf("taskComplete failed: %s", finalErr)
	}

	return
}

func updateTask(c appengine.Context, taskKey *datastore.Key, status TaskStatus, info string, result interface{}) (JobTask, error) {
	var task JobTask

	if err := datastore.Get(c, taskKey, &task); err != nil {
		return JobTask{}, err
	}

	task.UpdatedAt = time.Now()
	task.Info = info

	if status != "" {
		task.Status = status
	}

	if result != nil {
		resultBytes, err := json.Marshal(result)
		if err != nil {
			return JobTask{}, err
		}

		task.Result = string(resultBytes)
	}

	_, err := datastore.Put(c, taskKey, &task)
	return task, err
}

func GetJob(c appengine.Context, jobId int64) (JobInfo, error) {
	jobKey := datastore.NewKey(c, JobEntity, "", jobId, nil)
	var job JobInfo
	if err := datastore.Get(c, jobKey, &job); err != nil {
		return JobInfo{}, err
	}

	return job, nil
}

func GetJobResults(c appengine.Context, jobId int64) ([]interface{}, error) {
	jobKey := datastore.NewKey(c, JobEntity, "", jobId, nil)
	tasks, err := gatherTasks(c, jobKey, TaskTypeReduce)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(tasks))
	for i, task := range tasks {
		json.Unmarshal([]byte(task.Result), &result[i])
	}

	return result, nil
}

func RemoveJob(c appengine.Context, jobId int64) error {
	jobKey := datastore.NewKey(c, JobEntity, "", jobId, nil)
	q := datastore.NewQuery(TaskEntity).Ancestor(jobKey).KeysOnly()
	keys, err := q.GetAll(c, nil)
	if err != nil {
		return err
	}

	keys = append(keys, jobKey)

	return datastore.DeleteMulti(c, keys)
}

func gatherTasks(c appengine.Context, jobKey *datastore.Key, taskType TaskType) ([]JobTask, error) {
	// we don't use a filter here because it seems like a waste of a compound index
	q := datastore.NewQuery(TaskEntity).Ancestor(jobKey)
	var tasks []JobTask
	_, err := q.GetAll(c, &tasks)
	if err != nil {
		return nil, err
	}

	finalTasks := make([]JobTask, 0)
	for _, task := range tasks {
		if task.Type == taskType {
			finalTasks = append(finalTasks, task)
		}
	}

	return finalTasks, nil
}

// AppengineTaskQueue implements TaskInterface via appengine task queues
type AppengineTaskQueue struct {
	// StatusQueueName is the name of the queue used for tasks to report status to the controller
	StatusQueueName string
	// TaskQueueName is the name of the queue used to start new tasks
	TaskQueueName string
}

func (q AppengineTaskQueue) PostTask(c appengine.Context, taskUrl string, jsonParameters string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{"json": []string{jsonParameters}})
	_, err := taskqueue.Add(c, task, q.TaskQueueName)
	return err
}

func (q AppengineTaskQueue) PostStatus(c appengine.Context, taskUrl string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{})
	_, err := taskqueue.Add(c, task, q.StatusQueueName)
	return err
}

func retryTask(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key) error {
	var task JobTask
	var job JobInfo
	if err := datastore.Get(c, taskKey, &task); err != nil {
		return err
	} else if err := datastore.Get(c, taskKey.Parent(), &job); err != nil {
		return err
	} else if (task.Retries + 1) >= job.RetryCount {
		return fmt.Errorf("maxium retries exceeded")
	}

	task.Retries++
	if _, err := datastore.Put(c, taskKey, &task); err != nil {
		return err
	} else if err := pipeline.PostTask(c, task.Url, job.JsonParameters); err != nil {
		return err
	}

	c.Infof("retrying task %d/%d", task.Retries, job.RetryCount)

	return nil
}

func parseCompleteRequest(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) (*datastore.Key, bool, error) {
	var finalErr error = nil

	status := r.FormValue("status")

	switch status {
	case "":
		finalErr = fmt.Errorf("missing status for request %s", r)
	case "again":
		finalErr = retryTask(c, pipeline, taskKey)
		if finalErr == nil {
			return nil, true, nil
		}
		finalErr = fmt.Errorf("error retrying: %s (task failed due to: %s)", finalErr, r.FormValue("error"))
	case "error":
		finalErr = fmt.Errorf("failed task: %s", r.FormValue("error"))
	case "done":
	default:
		finalErr = fmt.Errorf("unknown job status %s", status)
	}

	jobKey := taskKey.Parent()

	if finalErr != nil {
		c.Errorf("bad status from task: %s", finalErr.Error())
		prevJob, _ := updateJobStage(c, jobKey, StageFailed)
		if prevJob.Stage == StageFailed {
			return nil, false, finalErr
		}

		if prevJob.OnCompleteUrl != "" {
			pipeline.PostStatus(c, fmt.Sprintf("%s?status=error;error=%s;id=%d", prevJob.OnCompleteUrl,
				url.QueryEscape(finalErr.Error()), jobKey.IntID()))
		}
		return nil, false, finalErr
	}

	return jobKey, false, nil
}

func jobFailed(c appengine.Context, pipeline MapReducePipeline, jobKey *datastore.Key, err error) {
	c.Errorf("%s", err)
	prevJob, _ := updateJobStage(c, jobKey, StageFailed)
	if prevJob.Stage == StageFailed {
		return
	}

	if prevJob.OnCompleteUrl != "" {
		pipeline.PostStatus(c, fmt.Sprintf("%s?status=error;error=%s;id=%d", prevJob.OnCompleteUrl,
			url.QueryEscape(err.Error()), jobKey.IntID()))
	}

	return
}
