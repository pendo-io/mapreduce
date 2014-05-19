package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"appengine/taskqueue"
	"encoding/json"
	"fmt"
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

type JobTask struct {
	Status    TaskStatus
	RunCount  int
	Url       string
	Info      string
	UpdatedAt time.Time
	Type      TaskType
	Result    string
}

type JobInfo struct {
	UrlPrefix     string
	Stage         JobStage
	UpdatedAt     time.Time
	TasksRunning  int
	OnCompleteUrl string
	WriterNames   []string `datastore:",noindex"`
}

type TaskInterface interface {
	PostTask(c appengine.Context, fullUrl string) error
}

type TaskType string

const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"
)

const JobEntity = "MapReduceJob"
const TaskEntity = "MapReduceTask"

func createJob(c appengine.Context, urlPrefix string, writerNames []string, onCompleteUrl string) (*datastore.Key, error) {
	key := datastore.NewKey(c, JobEntity, "", 0, nil)
	job := JobInfo{
		UrlPrefix:     urlPrefix,
		Stage:         StageFormation,
		UpdatedAt:     time.Now(),
		OnCompleteUrl: onCompleteUrl,
		WriterNames:   writerNames,
	}

	return datastore.Put(c, key, &job)
}

func createTasks(c appengine.Context, jobKey *datastore.Key, taskKeys []*datastore.Key, tasks []JobTask, newStage JobStage) error {
	err := datastore.RunInTransaction(c, func(c appengine.Context) error {
		var job JobInfo

		if err := datastore.Get(c, jobKey, &job); err != nil {
			return err
		}

		job.TasksRunning = len(tasks)
		job.Stage = newStage

		if len(job.WriterNames) > 2 {
			panic("here")
		}
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

	c.Criticalf("could not write status for job %d after multiple tries: %s", finalErr)
	return fmt.Errorf("failed to write new job status after multiple attempts: %s", finalErr)
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

	return
}

func updateTask(c appengine.Context, taskKey *datastore.Key, status TaskStatus, info string, result interface{}) error {
	var task JobTask

	if err := datastore.Get(c, taskKey, &task); err != nil {
		return err
	}

	task.UpdatedAt = time.Now()
	task.Status = status
	task.Info = info

	if result != nil {
		resultBytes, err := json.Marshal(result)
		if err != nil {
			return err
		}

		task.Result = string(resultBytes)
	}

	_, err := datastore.Put(c, taskKey, &task)
	return err
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

type AppengineTaskQueue struct {
	queueName string
}

func (q AppengineTaskQueue) PostTask(c appengine.Context, taskUrl string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{})
	_, err := taskqueue.Add(c, task, q.queueName)
	return err
}
