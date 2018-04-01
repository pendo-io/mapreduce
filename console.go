package mapreduce

import (
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pendo-io/appwrap"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const main = `<html><head><title>MapReduce Console</title><style>

table,td,th {
	border: 1px solid black;
}
</style>
</head>
<h1>MapReduce Console</h1>

<h2>Jobs</h2>

<table>
<tr>
    <th align="center">Id</th>
    <th align="center">Url</th>
    <th align="center">Stage</th>
    <th align="center">Start Time</th>
    <th align="center">Updated Time</th>
    <th align="center">Duration</th>
    <th></td>
</tr>

{{range $index, $job := .Jobs}}
<tr>
    {{$key := index $.Keys $index}} 
    <td>
        {{ $id:=$key.IntID }}
	<a href="job?id={{$id}}">{{$id}}</a>
    </td>
    <td>{{$job.UrlPrefix}}</td>
    <td align="center">{{$job.Stage}}</td>
    <td>{{$job.StartTime}}</td>
    <td>{{$job.UpdatedAt}}</td>
    <td>{{$job.Duration}}</td>
    <td><button onclick="location.href='delete?id={{$id}}'">Delete</button></td>
</tr>
{{end}}

</table>

`

const jobPage = `<html><head><title>MapReduce Console</title><style>

table,td,th {
	border: 1px solid black;
}
</style>
</head>
<h1>MapReduce Task</h1>

<p>Job Id {{.Id}}</p>
<p>{{.Pending}} Pending / {{.Running}} Running / {{.Done}} Done / {{.Failed }} Failed</p>

<table>
<tr>
    <th align="center">Id</th>
    <th align="center">Type</th>
    <th align="center">Status</th>
    <th align="center">Run Count</th>
    <th align="center">Start Time</th>
    <th align="center">Update Time</th>
    <th align="center">Info</th>
</tr>

{{range $index, $task := .Tasks}}
<tr>
    <td>
        {{$key := index $.TaskKeys $index}} {{ $key.IntID }}
    </td>
    <td align="center">{{$task.Type}}</td>
    <td align="center">{{$task.Status}}</td>
    <td align="center">{{$task.Retries}}</td>
    <td align="center">{{$task.StartTime}}</td>
    <td align="center">{{$task.UpdatedAt}}</td>
    <td align="center">{{$task.Info}}</td>
</tr>

{{end}}

</table>

`

func ConsoleHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	ds := appwrap.NewAppengineDatastore(c)

	if strings.HasSuffix(r.URL.Path, "/job") {
		id, _ := strconv.ParseInt(r.FormValue("id"), 10, 64)

		var tasks []JobTask
		var taskKeys []*datastore.Key
		if job, err := GetJob(ds, id); err != nil {
			http.Error(w, "Internal error reading job: "+err.Error(), http.StatusInternalServerError)
			return
		} else {
			switch job.Stage {
			case StageMapping, StageReducing:
				if tl, err := GetJobTasks(ds, job); err != nil {
					http.Error(w, "Internal error reading tasks: "+err.Error(), http.StatusInternalServerError)
					return
				} else {
					tasks = tl
					taskKeys = makeTaskKeys(ds, job.FirstTaskId, job.TaskCount)
				}
			default:
				jobKey := datastore.NewKey(c, JobEntity, "", id, nil)
				if tk, err := datastore.NewQuery(TaskEntity).Filter("Job =", jobKey).GetAll(c, &tasks); err != nil {
					http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
					return
				} else {
					taskKeys = tk
				}
			}
		}

		running := 0
		pending := 0
		done := 0
		failed := 0
		for i := range tasks {
			switch tasks[i].Status {
			case TaskStatusPending:
				pending++
			case TaskStatusRunning:
				running++
			case TaskStatusDone:
				done++
			case TaskStatusFailed:
				failed++
			}
		}

		t := template.New("main")
		t, _ = t.Parse(jobPage)
		if err := t.Execute(w, struct {
			Id                             int64
			Tasks                          []JobTask
			TaskKeys                       []*datastore.Key
			Pending, Running, Done, Failed int
		}{id, tasks, taskKeys, pending, running, done, failed}); err != nil {
			http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else if strings.HasSuffix(r.URL.Path, "/delete") {
		id, _ := strconv.ParseInt(r.FormValue("id"), 10, 64)

		if err := RemoveJob(ds, id); err != nil {
			http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		jobList(w, r, id)
	} else {
		jobList(w, r, 0)
	}
}

func jobList(w http.ResponseWriter, r *http.Request, skipId int64) {
	c := appengine.NewContext(r)

	q := datastore.NewQuery(JobEntity).Order("-UpdatedAt")
	var jobs []JobInfo
	keys, err := q.GetAll(c, &jobs)
	if err != nil {
		http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	type annotatedJob struct {
		JobInfo
		Duration time.Duration
	}
	annotatedList := make([]annotatedJob, 0, len(keys))

	for i, key := range keys {
		if key.IntID() != skipId {
			job := annotatedJob{JobInfo: jobs[i]}
			if !jobs[i].StartTime.IsZero() {
				job.Duration = jobs[i].UpdatedAt.Sub(jobs[i].StartTime)
			}

			annotatedList = append(annotatedList, job)
		}
	}

	log.Infof(c, "%d jobs %d annotatedList", len(jobs), len(annotatedList))

	t := template.New("main")
	t, _ = t.Parse(main)
	err = t.Execute(w, struct {
		Jobs []annotatedJob
		Keys []*datastore.Key
	}{annotatedList, keys})
	if err != nil {
		http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
