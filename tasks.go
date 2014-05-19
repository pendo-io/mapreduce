package kyrie

import (
	"appengine"
	"appengine/datastore"
	"encoding/json"
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
)

type JobTask struct {
	Status    TaskStatus
	RunCount  int
	Url       string
	Info      string
	UpdatedAt time.Time
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
	PostTask(fullUrl string) error
}

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

func taskComplete(c appengine.Context, jobKey *datastore.Key, expectedStage, nextStage JobStage) (stageChanged bool, job JobInfo, finalErr error) {
	finalErr = datastore.RunInTransaction(c, func(c appengine.Context) error {
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
	}, nil)

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

func gatherTasks(c appengine.Context, jobKey *datastore.Key) ([]JobTask, error) {
	q := datastore.NewQuery(TaskEntity).Ancestor(jobKey)
	var tasks []JobTask
	_, err := q.GetAll(c, &tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}
