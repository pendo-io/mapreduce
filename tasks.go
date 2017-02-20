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
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/pendo-io/appwrap"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/taskqueue"
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
	StartTime           time.Time
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
	PostTask(c context.Context, fullUrl string, jsonParameters string) error
	PostStatus(c context.Context, fullUrl string) error
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

// this is returned when multiple monitors conflict; only the conflicting monitor complains
var errMonitorJobConflict = fmt.Errorf("monitor job conflict detected")

func createJob(ds appwrap.Datastore, urlPrefix string, writerNames []string, onCompleteUrl string, separateReduceItems bool, jsonParameters string, retryCount int) (*datastore.Key, error) {
	if retryCount == 0 {
		// default
		retryCount = 3
	}

	key := ds.NewKey(JobEntity, "", 0, nil)
	job := JobInfo{
		UrlPrefix:           urlPrefix,
		Stage:               StageFormation,
		UpdatedAt:           time.Now(),
		StartTime:           time.Now(),
		OnCompleteUrl:       onCompleteUrl,
		SeparateReduceItems: separateReduceItems,
		WriterNames:         writerNames,
		RetryCount:          retryCount,
		JsonParameters:      jsonParameters,
	}

	return ds.Put(key, &job)
}

func createTasks(ds appwrap.Datastore, jobKey *datastore.Key, taskKeys []*datastore.Key, tasks []JobTask, newStage JobStage, log appwrap.Logging) error {
	now := time.Now()
	firstId := taskKeys[0].IntID()
	for i := range tasks {
		tasks[i].StartTime = now
		tasks[i].Job = jobKey

		if taskKeys[i].IntID() < firstId {
			firstId = taskKeys[i].IntID()
		}
	}

	putSize := 64

	log.Infof("creating %d %s tasks", len(tasks), tasks[0].Type)

	i := 0
	for i < len(tasks) {
		if err := backoff.Retry(func() error {
			last := i + putSize
			if last > len(tasks) {
				last = len(tasks)
			}

			if _, err := ds.PutMulti(taskKeys[i:last], tasks[i:last]); err != nil {
				if putSize > 5 {
					putSize /= 2
				}

				log.Infof("shrinkning putSize to %d because of %s", putSize, err)

				return err
			}

			i = last

			return nil
		}, mrBackOff()); err != nil {
			log.Errorf("hit backoff error %s", err)
			return err
		}
	}

	log.Infof("tasks created; first is %s", taskKeys[0])

	return runInTransaction(ds,
		func(ds appwrap.Datastore) error {
			var job JobInfo

			if err := ds.Get(jobKey, &job); err != nil {
				return err
			}

			job.TaskCount = len(tasks)
			job.FirstTaskId = firstId
			job.Stage = newStage

			_, err := ds.Put(jobKey, &job)
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

func runInTransaction(ds appwrap.Datastore, f func(ds appwrap.Datastore) error) error {
	return backoff.Retry(func() error {
		return ds.RunInTransaction(f, nil)
	}, mrBackOff())
}

func markJobFailed(c context.Context, ds appwrap.Datastore, jobKey *datastore.Key, log appwrap.Logging) (prev JobInfo, finalErr error) {
	finalErr = runInTransaction(ds, func(ds appwrap.Datastore) error {
		prev = JobInfo{}
		if err := ds.Get(jobKey, &prev); err != nil {
			return err
		}

		job := prev
		job.Stage = StageFailed

		_, err := ds.Put(jobKey, &job)
		return err
	})

	if finalErr != nil {
		log.Criticalf("marking job failed for key %s failed: %s", jobKey, finalErr)
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
func jobStageComplete(ds appwrap.Datastore, jobKey *datastore.Key, taskKeys []*datastore.Key, expectedStage, nextStage JobStage, log appwrap.Logging) (stageChanged bool, job JobInfo, finalErr error) {
	last := len(taskKeys)
	tasks := make([]JobTask, 100)
	for last > 0 {
		first := last - 100
		if first < 0 {
			first = 0
		}

		taskCount := last - first

		if err := ds.GetMulti(taskKeys[first:last], tasks[0:taskCount]); err != nil {
			finalErr = err
			return
		} else {
			for i := 0; i < taskCount; i++ {
				if tasks[i].Status == TaskStatusFailed {
					log.Infof("failed tasks found")
					nextStage = StageFailed
					last = -1
					finalErr = taskError{tasks[i].Info}
					break
				} else if tasks[i].Status != TaskStatusDone {
					return
				}
			}

			if last >= 0 {
				last = first
			}
		}
	}

	// running this in a transaction ensures only one process advances the stage
	if transErr := runInTransaction(ds, func(ds appwrap.Datastore) error {
		job = JobInfo{}
		if err := ds.Get(jobKey, &job); err != nil {
			return err
		}

		if job.Stage != expectedStage {
			// we're not where we expected, so advancing this isn't our responsibility
			stageChanged = false
			return errMonitorJobConflict
		}

		job.Stage = nextStage
		job.UpdatedAt = time.Now()

		_, err := ds.Put(jobKey, &job)
		stageChanged = (err == nil)
		return err
	}); transErr != nil {
		finalErr = transErr
	}

	if finalErr != nil {
		log.Criticalf("taskComplete failed: %s", finalErr)
	} else {
		log.Infof("task is complete")
	}

	return
}

func getTask(ds appwrap.Datastore, taskKey *datastore.Key) (JobTask, error) {
	var task JobTask

	err := backoff.Retry(func() error {
		return ds.Get(taskKey, &task)
	}, mrBackOff())

	if err != nil {
		return JobTask{}, err
	}

	return task, nil
}

func updateTask(ds appwrap.Datastore, taskKey *datastore.Key, status TaskStatus, tryIncrement int, info string, result interface{}) (JobTask, error) {
	var task JobTask

	newCount := -1

	err := backoff.Retry(func() error {
		if err := ds.Get(taskKey, &task); err != nil {
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

		_, err := ds.Put(taskKey, &task)
		return err
	}, mrBackOff())

	return task, err
}

func getJob(ds appwrap.Datastore, jobKey *datastore.Key) (JobInfo, error) {
	var job JobInfo

	err := backoff.Retry(func() error {
		if err := ds.Get(jobKey, &job); err != nil {
			return err
		}

		return nil
	}, mrBackOff())

	job.Id = jobKey.IntID()

	return job, err
}

func GetJob(ds appwrap.Datastore, jobId int64) (JobInfo, error) {
	return getJob(ds, ds.NewKey(JobEntity, "", jobId, nil))
}

func GetJobTasks(ds appwrap.Datastore, job JobInfo) ([]JobTask, error) {
	if tasks, err := gatherTasks(ds, job); err != nil {
		return nil, err
	} else {
		return tasks, nil
	}
}

func GetJobTaskResults(ds appwrap.Datastore, job JobInfo) ([]interface{}, error) {
	if tasks, err := gatherTasks(ds, job); err != nil {
		return nil, err
	} else {
		result := make([]interface{}, len(tasks))
		for i, task := range tasks {
			json.Unmarshal([]byte(task.Result), &result[i])
		}

		return result, nil
	}
}

func RemoveJob(ds appwrap.Datastore, jobId int64) error {
	jobKey := ds.NewKey(JobEntity, "", jobId, nil)
	q := ds.NewQuery(TaskEntity).Filter("Job =", jobKey).KeysOnly()
	keys, err := q.GetAll(nil)
	if err != nil {
		return err
	}

	keys = append(keys, jobKey)

	i := 0
	for i < len(keys) {
		last := i + 250
		if last > len(keys) {
			last = len(keys)
		}

		if err := ds.DeleteMulti(keys[i:last]); err != nil {
			return err
		}

		i = last
	}

	return nil
}

func makeTaskKeys(ds appwrap.Datastore, firstId int64, count int) []*datastore.Key {
	taskKeys := make([]*datastore.Key, count)
	for i := 0; i < count; i++ {
		taskKeys[i] = ds.NewKey(TaskEntity, "", firstId+int64(i), nil)
	}

	return taskKeys
}

func gatherTasks(ds appwrap.Datastore, job JobInfo) ([]JobTask, error) {
	taskKeys := makeTaskKeys(ds, job.FirstTaskId, job.TaskCount)
	tasks := make([]JobTask, len(taskKeys))

	i := 0
	for i < len(taskKeys) {
		last := i + 100
		if last > len(taskKeys) {
			last = len(taskKeys)
		}

		if err := ds.GetMulti(taskKeys[i:last], tasks[i:last]); err != nil {
			return nil, err
		}

		i = last
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

func (q AppengineTaskQueue) PostTask(c context.Context, taskUrl string, jsonParameters string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{"json": []string{jsonParameters}})
	_, err := taskqueue.Add(c, task, q.TaskQueueName)
	return err
}

func (q AppengineTaskQueue) PostStatus(c context.Context, taskUrl string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{})
	_, err := taskqueue.Add(c, task, q.StatusQueueName)
	return err
}

func retryTask(c context.Context, ds appwrap.Datastore, taskIntf TaskInterface, jobKey *datastore.Key, taskKey *datastore.Key, log appwrap.Logging) error {
	var job JobInfo

	if j, err := getJob(ds, jobKey); err != nil {
		return fmt.Errorf("getting job: %s", err)
	} else {
		job = j
	}

	time.Sleep(time.Duration(job.RetryCount) * 5 * time.Second)

	if err := backoff.Retry(func() error {
		var task JobTask
		if err := ds.Get(taskKey, &task); err != nil {
			return fmt.Errorf("getting task: %s", err)
		}

		task.Status = TaskStatusPending
		if _, err := ds.Put(taskKey, &task); err != nil {
			return fmt.Errorf("putting task: %s", err)
		} else if err := taskIntf.PostTask(c, task.Url, job.JsonParameters); err != nil {
			return fmt.Errorf("enqueuing task: %s", err)
		}

		log.Infof("retrying task %d/%d", task.Retries, job.RetryCount)
		return nil
	}, mrBackOff()); err != nil {
		log.Infof("retryTask() failed after backoff attempts")
		return err
	} else {
		return nil
	}
}

func jobFailed(c context.Context, ds appwrap.Datastore, taskIntf TaskInterface, jobKey *datastore.Key, err error, log appwrap.Logging) {
	log.Errorf("jobFailed: %s", err)
	prevJob, _ := markJobFailed(c, ds, jobKey, log) // this might mark it failed again. whatever.

	if prevJob.OnCompleteUrl != "" {
		taskIntf.PostStatus(c, fmt.Sprintf("%s?status=error;error=%s;id=%d", prevJob.OnCompleteUrl,
			url.QueryEscape(err.Error()), jobKey.IntID()))
	}

	return
}

// waitForStageCompletion() is split up like this for testability
type jobStageCompletionFunc func(ds appwrap.Datastore, jobKey *datastore.Key, taskKeys []*datastore.Key, expectedStage, nextStage JobStage, log appwrap.Logging) (stageChanged bool, job JobInfo, finalErr error)

func waitForStageCompletion(c context.Context, ds appwrap.Datastore, taskIntf TaskInterface, jobKey *datastore.Key, currentStage, nextStage JobStage, timeout time.Duration, log appwrap.Logging) (JobInfo, error) {
	return doWaitForStageCompletion(c, ds, taskIntf, jobKey, currentStage, nextStage, 5*time.Second, jobStageComplete, timeout, log)
}

// if err != nil, this failed (which should never happen, and should be considered fatal)
func doWaitForStageCompletion(c context.Context, ds appwrap.Datastore, taskIntf TaskInterface, jobKey *datastore.Key, currentStage, nextStage JobStage, delay time.Duration, checkCompletion jobStageCompletionFunc, timeout time.Duration, log appwrap.Logging) (JobInfo, error) {
	var job JobInfo
	var taskKeys []*datastore.Key

	if j, err := getJob(ds, jobKey); err != nil {
		log.Criticalf("monitor failed to load job: %s", err)
		//http.Error(w, "error loading job", 500)
		return JobInfo{}, err
	} else {
		job = j
		taskKeys = makeTaskKeys(ds, job.FirstTaskId, job.TaskCount)
	}

	start := time.Now()

	for time.Now().Sub(start) < timeout {
		stageChanged, newJob, err := checkCompletion(ds, jobKey, taskKeys, currentStage, nextStage, log)
		if err == errMonitorJobConflict {
			log.Errorf("monitor job conflict detected")
			return JobInfo{}, err
		} else if !stageChanged {
			// this ignores errors.. it should instead sleep a bit longer and maybe even resubmit the
			// monitor job

			if err != nil {
				log.Errorf("error getting map task complete status: %s", err.Error())
			}

			time.Sleep(delay)
		} else if newJob.Stage == StageFailed {
			// we found a failed task; the job has been marked as failed; notify the caller and exit
			jobFailed(c, ds, taskIntf, jobKey, err, log)
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

	log.Infof("after wait job status is %s", job.Stage)

	return job, nil
}

type startTopIntf interface {
	TaskInterface
	TaskStatusChange
}

// returns job if err is nil, err, and a boolean saying if the task should be restarted (true/false)
func startTask(c context.Context, ds appwrap.Datastore, taskIntf startTopIntf, taskKey *datastore.Key, log appwrap.Logging) (JobTask, error, bool) {
	if task, err := getTask(ds, taskKey); err != nil {
		return JobTask{}, fmt.Errorf("failed to get task status: %s", err), true
	} else if job, err := getJob(ds, task.Job); err != nil {
		return JobTask{}, fmt.Errorf("failed to get job: %s", err), true
	} else if task.Retries > job.RetryCount {
		// we've failed
		if _, err := updateTask(ds, taskKey, TaskStatusFailed, 0, "maxium retries exceeeded", nil); err != nil {
			return JobTask{}, fmt.Errorf("Could not update task with failure: %s", err), true
		}

		return JobTask{}, fmt.Errorf("maximum retries exceeded"), false
	} else {
		if task.Status == TaskStatusRunning {
			// we think we're already running, but we got here. that means we failed
			// unexpectedly.
			log.Infof("restarted automatically -- running again")
		} else if task.Status == TaskStatusFailed {
			log.Infof("started even though we've already failed. interesting")
			return JobTask{}, fmt.Errorf("restarted failed task"), false
		} else if _, err := updateTask(ds, taskKey, TaskStatusRunning, 1, "", nil); err != nil {
			return JobTask{}, fmt.Errorf("failed to update map task to running: %s", err), true
		}

		taskIntf.Status(task.Job.IntID(), task)
		return task, nil, false
	}
}

func endTask(c context.Context, ds appwrap.Datastore, taskIntf startTopIntf, jobKey *datastore.Key, taskKey *datastore.Key, resultErr error, result interface{}, log appwrap.Logging) error {
	if resultErr == nil {
		if task, err := updateTask(ds, taskKey, TaskStatusDone, 0, "", result); err != nil {
			return fmt.Errorf("Could not update task: %s", err)
		} else {
			taskIntf.Status(jobKey.IntID(), task)
		}
	} else {
		if _, ok := resultErr.(tryAgainError); ok {
			// wasn't fatal, go for it
			if retryErr := retryTask(c, ds, taskIntf, jobKey, taskKey, log); retryErr != nil {
				return fmt.Errorf("error retrying: %s (task failed due to: %s)", retryErr, resultErr)
			} else {
				log.Infof("retrying task due to %s", resultErr)
				return nil
			}
		}

		// fatal error
		if _, err := updateTask(ds, taskKey, TaskStatusFailed, 0, resultErr.Error(), nil); err != nil {
			return fmt.Errorf("Could not update task with failure: %s", err)
		}
	}

	return nil
}
