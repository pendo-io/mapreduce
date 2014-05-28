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

type JobTask struct {
	Status    TaskStatus
	RunCount  int
	Url       string `datastore:",noindex"`
	Info      string
	StartTime time.Time
	UpdatedAt time.Time
	Type      TaskType
	Result    string
	Retries   int
}

type JobInfo struct {
	UrlPrefix     string
	Stage         JobStage
	UpdatedAt     time.Time
	TasksRunning  int
	RetryCount    int
	OnCompleteUrl string
	WriterNames   []string `datastore:",noindex"`
}

// TaskInterface defines how the map and reduce tasks and controlled, and how they report
// their status.
type TaskInterface interface {
	PostTask(c appengine.Context, fullUrl string) error
	PostStatus(c appengine.Context, fullUrl string) error
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

func updateTask(c appengine.Context, taskKey *datastore.Key, status TaskStatus, info string, result interface{}) error {
	var task JobTask

	if err := datastore.Get(c, taskKey, &task); err != nil {
		return err
	}

	task.UpdatedAt = time.Now()
	task.Info = info

	if status != "" {
		task.Status = status
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
}

func GetJobResults(c appengine.Context, jobKey *datastore.Key) ([]interface{}, error) {
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

func (q AppengineTaskQueue) PostTask(c appengine.Context, taskUrl string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{})
	_, err := taskqueue.Add(c, task, q.TaskQueueName)
	return err
}

func (q AppengineTaskQueue) PostStatus(c appengine.Context, taskUrl string) error {
	task := taskqueue.NewPOSTTask(taskUrl, url.Values{})
	_, err := taskqueue.Add(c, task, q.StatusQueueName)
	return err
}

func parseCompleteRequest(c appengine.Context, pipeline MapReducePipeline, taskKey *datastore.Key, r *http.Request) (*datastore.Key, error) {
	var finalErr error = nil

	status := r.FormValue("status")

	switch status {
	case "":
		finalErr = fmt.Errorf("missing status for request %s", r)
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
			return nil, finalErr
		}

		pipeline.PostStatus(c, fmt.Sprintf("%s?status=error;error=%s;id=%d", prevJob.OnCompleteUrl,
			url.QueryEscape(finalErr.Error()), jobKey.IntID()))
		return nil, finalErr
	}

	return jobKey, nil
}
