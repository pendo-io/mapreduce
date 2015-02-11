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
	"github.com/cenkalti/backoff"
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
	Status              TaskStatus `datastore:,noindex`
	Job                 *datastore.Key
	Done                *datastore.Key // nil if the task isn't done, job if it is
	Info                string         `datastore:,"noindex"`
	StartTime           time.Time      `datastore:,"noindex"`
	UpdatedAt           time.Time      `datastore:,"noindex"`
	Type                TaskType       `datastore:,"noindex"`
	Retries             int            `datastore:,"noindex"`
	SeparateReduceItems bool
	// this is named intermediate storage sources, and only used for reduce tasks
	ReadFrom []byte `datastore:",noindex"`
	Url      string `datastore:",noindex"`
	Result   string `datastore:",noindex"`
}

// JobInfo is the entity stored in the datastore defining the MapReduce Job
type JobInfo struct {
	UrlPrefix           string
	Stage               JobStage
	UpdatedAt           time.Time
	TaskCount           int      `datastore:"TasksRunning,noindex"`
	FirstTaskId         int64    `datastore:",noindex"`
	RetryCount          int      `datastore:",noindex"`
	SeparateReduceItems bool     `datastore:",noindex"`
	OnCompleteUrl       string   `datastore:",noindex"`
	WriterNames         []string `datastore:",noindex"`
	JsonParameters      string   `datastore:",noindex"`

	// filled in by getJob
	Id int64 `datastore:"-"`
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

func createJob(c appengine.Context, urlPrefix string, writerNames []string, onCompleteUrl string, separateReduceItems bool, jsonParameters string, retryCount int) (*datastore.Key, error) {
	if retryCount == 0 {
		// default
		retryCount = 3
	}

	key := datastore.NewKey(c, JobEntity, "", 0, nil)
	job := JobInfo{
		UrlPrefix:           urlPrefix,
		Stage:               StageFormation,
		UpdatedAt:           time.Now(),
		OnCompleteUrl:       onCompleteUrl,
		SeparateReduceItems: separateReduceItems,
		WriterNames:         writerNames,
		RetryCount:          retryCount,
		JsonParameters:      jsonParameters,
	}

	return datastore.Put(c, key, &job)
}

func createTasks(c appengine.Context, jobKey *datastore.Key, taskKeys []*datastore.Key, tasks []JobTask, newStage JobStage) error {
	now := time.Now()
	firstId := taskKeys[0].IntID()
	for i := range tasks {
		tasks[i].StartTime = now
		tasks[i].Job = jobKey

		if taskKeys[i].IntID() < firstId {
			firstId = taskKeys[i].IntID()
		}
	}

	if err := backoff.Retry(func() error {
		_, err := datastore.PutMulti(c, taskKeys, tasks)
		return err
	}, mrBackOff()); err != nil {
		return err
	}

	return runInTransaction(c,
		func(c appengine.Context) error {
			var job JobInfo

			if err := datastore.Get(c, jobKey, &job); err != nil {
				return err
			}

			job.TaskCount = len(tasks)
			job.FirstTaskId = firstId
			job.Stage = newStage

			_, err := datastore.Put(c, jobKey, &job)
			return err
		})
}

func mrBackOff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 10 * time.Millisecond
	b.MaxInterval = 10 * time.Second
	b.MaxElapsedTime = 90 * time.Second

	return b
}

func runInTransaction(c appengine.Context, f func(c appengine.Context) error) error {
	return backoff.Retry(func() error {
		return datastore.RunInTransaction(c, f, nil)
	}, mrBackOff())
}

func markJobFailed(c appengine.Context, jobKey *datastore.Key) (prev JobInfo, finalErr error) {
	finalErr = runInTransaction(c, func(c appengine.Context) error {
		prev = JobInfo{}
		if err := datastore.Get(c, jobKey, &prev); err != nil {
			return err
		}

		job := prev
		job.Stage = StageFailed

		_, err := datastore.Put(c, jobKey, &job)
		return err
	})

	if finalErr != nil {
		c.Criticalf("marking job failed for key %s failed: %s", jobKey, finalErr)
	}

	return
}

type taskError struct{ err string }

func (t taskError) Error() string { return "taskError " + t.err }

// check if the specified job has completed. it should currently be at expectedStage, and if it's been completed
// we advance it to next stage. if it's already at nextStage another process has beaten us to it so we're done
//
// caller needs to check the stage in the final job; if stageChanged is true it will be either nextStage or StageFailed.
// If StageFailed then at least one of the underlying tasks failed and the reason will appear as a taskError{} in err
func jobStageComplete(c appengine.Context, jobKey *datastore.Key, taskKeys []*datastore.Key, expectedStage, nextStage JobStage) (stageChanged bool, job JobInfo, finalErr error) {
	if c, err := datastore.NewQuery(TaskEntity).Filter("Done =", nil).Limit(1).KeysOnly().Count(c); err != nil {
		finalErr = err
		return
	} else if c != 0 {
		return
	}

	tasks := make([]JobTask, len(taskKeys))
	// gatherTasks is eventually consistent. that's okay here, because
	// we'll eventually get TaskStatusFailed or TaskStatusDone
	if err := datastore.GetMulti(c, taskKeys, tasks); err != nil {
		finalErr = err
		return
	} else {
		for i := range tasks {
			if tasks[i].Status == TaskStatusFailed {
				nextStage = StageFailed
				finalErr = taskError{tasks[i].Info}
				break
			}
		}
	}

	// running this in a transaction ensures only one process advances the stage
	if transErr := runInTransaction(c, func(c appengine.Context) error {
		job = JobInfo{}
		if err := datastore.Get(c, jobKey, &job); err != nil {
			return err
		}

		if job.Stage != expectedStage {
			// we're not where we expected, so advancing this isn't our responsibility
			stageChanged = false
			return nil
		}

		job.Stage = nextStage
		job.UpdatedAt = time.Now()

		_, err := datastore.Put(c, jobKey, &job)
		stageChanged = (err == nil)
		return err
	}); transErr != nil {
		finalErr = transErr
	}

	if finalErr != nil {
		c.Criticalf("taskComplete failed: %s", finalErr)
	}

	return
}

func getTask(c appengine.Context, taskKey *datastore.Key) (JobTask, error) {
	var task JobTask

	err := backoff.Retry(func() error {
		return datastore.Get(c, taskKey, &task)
	}, mrBackOff())

	if err != nil {
		return JobTask{}, err
	}

	return task, nil
}

func updateTask(c appengine.Context, taskKey *datastore.Key, status TaskStatus, tryIncrement int, info string, result interface{}) (JobTask, error) {
	var task JobTask

	newCount := -1

	err := backoff.Retry(func() error {
		if err := datastore.Get(c, taskKey, &task); err != nil {
			return err
		}

		task.UpdatedAt = time.Now()
		task.Info = info

		// this prevents double incrementing if the Put times out but has actually
		// written the value
		if newCount == -1 {
			newCount = task.Retries + tryIncrement
		}
		task.Retries = newCount

		if status != "" {
			task.Status = status
			if status == TaskStatusDone || task.Status == TaskStatusFailed {
				task.Done = task.Job
			}
		}

		if result != nil {
			resultBytes, err := json.Marshal(result)
			if err != nil {
				return err
			}

			task.Result = string(resultBytes)
		}

		_, err := datastore.Put(c, taskKey, &task)
		return err
	}, mrBackOff())

	return task, err
}

func getJob(c appengine.Context, jobKey *datastore.Key) (JobInfo, error) {
	var job JobInfo

	err := backoff.Retry(func() error {
		if err := datastore.Get(c, jobKey, &job); err != nil {
			return err
		}

		return nil
	}, mrBackOff())

	job.Id = jobKey.IntID()

	return job, err
}

func GetJob(c appengine.Context, jobId int64) (JobInfo, error) {
	return getJob(c, datastore.NewKey(c, JobEntity, "", jobId, nil))
}

func GetJobTasks(c appengine.Context, job JobInfo) ([]JobTask, error) {
	if tasks, err := gatherTasks(c, job); err != nil {
		return nil, err
	} else {
		return tasks, nil
	}
}

func GetJobTaskResults(c appengine.Context, job JobInfo) ([]interface{}, error) {
	if tasks, err := gatherTasks(c, job); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(tasks))
		for i, task := range tasks {
			json.Unmarshal([]byte(task.Result), &result[i])
		}

		return result, nil
	}
}

func RemoveJob(c appengine.Context, jobId int64) error {
	jobKey := datastore.NewKey(c, JobEntity, "", jobId, nil)
	q := datastore.NewQuery(TaskEntity).Filter("Job =", jobKey).KeysOnly()
	keys, err := q.GetAll(c, nil)
	if err != nil {
		return err
	}

	keys = append(keys, jobKey)

	return datastore.DeleteMulti(c, keys)
}

func makeTaskKeys(c appengine.Context, firstId int64, count int) []*datastore.Key {
	taskKeys := make([]*datastore.Key, count)
	for i := 0; i < count; i++ {
		taskKeys[i] = datastore.NewKey(c, TaskEntity, "", firstId+int64(i), nil)
	}

	return taskKeys
}

func gatherTasks(c appengine.Context, job JobInfo) ([]JobTask, error) {
	taskKeys := makeTaskKeys(c, job.FirstTaskId, job.TaskCount)
	tasks := make([]JobTask, len(taskKeys))
	if err := datastore.GetMulti(c, taskKeys, tasks); err != nil {
		return nil, err
	}

	return tasks, nil
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

func retryTask(c appengine.Context, taskIntf TaskInterface, jobKey *datastore.Key, taskKey *datastore.Key) error {
	var task JobTask
	var job JobInfo
	if err := datastore.Get(c, taskKey, &task); err != nil {
		return err
	} else if err := datastore.Get(c, jobKey, &job); err != nil {
		return err
	}

	task.Status = TaskStatusPending
	if _, err := datastore.Put(c, taskKey, &task); err != nil {
		return err
	} else if err := taskIntf.PostTask(c, task.Url, job.JsonParameters); err != nil {
		return err
	}

	c.Infof("retrying task %d/%d", task.Retries, job.RetryCount)

	return nil
}

func jobFailed(c appengine.Context, taskIntf TaskInterface, jobKey *datastore.Key, err error) {
	c.Errorf("jobFailed: %s", err)
	prevJob, _ := markJobFailed(c, jobKey)
	if prevJob.Stage == StageFailed {
		return
	}

	if prevJob.OnCompleteUrl != "" {
		taskIntf.PostStatus(c, fmt.Sprintf("%s?status=error;error=%s;id=%d", prevJob.OnCompleteUrl,
			url.QueryEscape(err.Error()), jobKey.IntID()))
	}

	return
}

// waitForStageCompletion() is split up like this for testability
type jobStageCompletionFunc func(c appengine.Context, jobKey *datastore.Key, taskKeys []*datastore.Key, expectedStage, nextStage JobStage) (stageChanged bool, job JobInfo, finalErr error)

func waitForStageCompletion(c appengine.Context, taskIntf TaskInterface, jobKey *datastore.Key, currentStage, nextStage JobStage, timeout time.Duration) (JobInfo, error) {
	return doWaitForStageCompletion(c, taskIntf, jobKey, currentStage, nextStage, 5*time.Second, jobStageComplete, timeout)
}

// if err != nil, this failed (which should never happen, and should be considered fatal)
func doWaitForStageCompletion(c appengine.Context, taskIntf TaskInterface, jobKey *datastore.Key, currentStage, nextStage JobStage, delay time.Duration, checkCompletion jobStageCompletionFunc, timeout time.Duration) (JobInfo, error) {
	var job JobInfo
	var taskKeys []*datastore.Key

	if j, err := getJob(c, jobKey); err != nil {
		c.Criticalf("monitor failed to load job: %s", err)
		//http.Error(w, "error loading job", 500)
		return JobInfo{}, err
	} else {
		job = j
		taskKeys = makeTaskKeys(c, job.FirstTaskId, job.TaskCount)
	}

	start := time.Now()

	for time.Now().Sub(start) < timeout {
		stageChanged, newJob, err := checkCompletion(c, jobKey, taskKeys, currentStage, nextStage)
		if !stageChanged {
			// this ignores errors.. it should instead sleep a bit longer and maybe even resubmit the
			// monitor job

			if err != nil {
				c.Errorf("error getting map task complete status: %s", err.Error())
			}

			time.Sleep(delay)
		} else if newJob.Stage == StageFailed {
			// we found a failed task; the job has been marked as failed; notify the caller and exit
			jobFailed(c, taskIntf, jobKey, err)
			return job, fmt.Errorf("failed task")
		} else if err != nil {
			// this really shouldn't happen.
			err := fmt.Errorf("error getting map task complete status even though stage was changed!!: %s", err.Error())
			return job, err
		} else {
			job = newJob
			break
		}
	}

	c.Infof("after wait job status is %s", job.Stage)

	return job, nil
}

type startTopIntf interface {
	TaskInterface
	TaskStatusChange
}

// returns job if err is nil, err, and a boolean saying if the task should be restarted (true/false)
func startTask(c appengine.Context, taskIntf startTopIntf, taskKey *datastore.Key) (JobTask, error, bool) {
	if task, err := getTask(c, taskKey); err != nil {
		return JobTask{}, fmt.Errorf("failed to get task status: %s", err), true
	} else if job, err := getJob(c, task.Job); err != nil {
		return JobTask{}, fmt.Errorf("failed to get job: %s", err), true
	} else if task.Retries > job.RetryCount {
		// we've failed
		if _, err := updateTask(c, taskKey, TaskStatusFailed, 0, "maxium retries exceeeded", nil); err != nil {
			return JobTask{}, fmt.Errorf("Could not update task with failure: %s", err), true
		}

		return JobTask{}, fmt.Errorf("maximum retries exceeded"), false
	} else {
		if task.Status == TaskStatusRunning {
			// we think we're already running, but we got here. that means we failed
			// unexpectedly.
			c.Infof("restarted automatically -- running again")
		} else if task.Status == TaskStatusFailed {
			c.Infof("started even though we've already failed. interesting")
			return JobTask{}, fmt.Errorf("restarted failed task"), false
		} else if _, err := updateTask(c, taskKey, TaskStatusRunning, 1, "", nil); err != nil {
			return JobTask{}, fmt.Errorf("failed to update map task to running: %s", err), true
		}

		taskIntf.Status(task.Job.IntID(), task)
		return task, nil, false
	}
}

func endTask(c appengine.Context, taskIntf startTopIntf, jobKey *datastore.Key, taskKey *datastore.Key, resultErr error, result interface{}) error {
	if resultErr == nil {
		if task, err := updateTask(c, taskKey, TaskStatusDone, 0, "", result); err != nil {
			return fmt.Errorf("Could not update task: %s", err)
		} else {
			taskIntf.Status(jobKey.IntID(), task)
		}
	} else {
		if _, ok := resultErr.(tryAgainError); ok {
			// wasn't fatal, go for it
			if retryErr := retryTask(c, taskIntf, jobKey, taskKey); retryErr != nil {
				resultErr = fmt.Errorf("error retrying: %s (task failed due to: %s)", retryErr, resultErr)
			} else {
				c.Infof("retrying task due to %s", resultErr)
				return nil
			}
		}

		// fatal error
		if _, err := updateTask(c, taskKey, TaskStatusFailed, 0, resultErr.Error(), nil); err != nil {
			return fmt.Errorf("Could not update task with failure: %s", err)
		}
	}

	return nil
}
