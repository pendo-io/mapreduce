package mapreduce

import (
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pendo-io/appwrap"
	"google.golang.org/appengine"
)

const head = `<html><head><title>MapReduce Console</title>`

const main = `$(function() {
        // call the tablesorter plugin
        $("table").tablesorter({
                headers: { 0: { sorter: 'integer' },
                           //1: { sorter: false },
                           2: { sorter: false },
                           3: { sorter: 'isoDatetime' },
                           4: { sorter: 'isoDatetime' },
                           5: { sorter: 'duration' },
                           6: { sorter: false },
                },
                sortList: [[3,1]],
                //debug: true,
                cancelSelection: false,
                widgets: ['filter', 'zebra'],
                widthFixed: true,
        });
});
</script>
</head>
<body>
<h1>MapReduce Console</h1>

<h2>Jobs</h2>

<table>
<thead>
<tr>
    <th align="center">Id</th>
    <th align="center">Url<br /><input onchange='$(this).parents("table").trigger("applyWidgets")'></input></th>
    <th align="center">Stage<br/><select multiple onchange='$(this).parents("table").trigger("applyWidgets")'></select></th>
    <th align="center">Start Time</th>
    <th align="center">Updated Time</th>
    <th align="center">Duration</th>
    <th></td>
</tr>
</thead>
<tbody>
{{range $index, $job := .Jobs}}
<tr>
    {{$key := index $.Keys $index}} 
    <td>
        {{ $id:=(KeyIntId $key) }}
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
</tbody>
</table>
</body>
</html>

`

const jobPage = `$(function() {
        // call the tablesorter plugin
        $("table").tablesorter({
                headers: { 0: { sorter: 'integer' },
                           1: { sorter: false },
                           2: { sorter: false },
			   3: { sorter: 'interger' },
                           4: { sorter: 'isoDatetime' },
                           5: { sorter: 'isoDatetime' },
                           //6: { sorter: false },
                },
                sortList: [[0,0]],
                //debug: true,
                cancelSelection: false,
                widgets: ['filter', 'zebra'],
                widthFixed: true,
        });
});
</script>
</head>
<body>
<h1>MapReduce Task</h1>

<p>Job Id {{.Id}}</p>
<p>{{.Pending}} Pending / {{.Running}} Running / {{.Done}} Done / {{.Failed }} Failed</p>

<table>
<thead>
<tr>
    <th align="center">Id</th>
    <th align="center">Type<br/><select multiple onchange='$(this).parents("table").trigger("applyWidgets")'></select></th>
    <th align="center">Status<br/><select multiple onchange='$(this).parents("table").trigger("applyWidgets")'></select></th>
    <th align="center">Run Count<br/><select multiple onchange='$(this).parents("table").trigger("applyWidgets")'></select></th>
    <th align="center">Start Time</th>
    <th align="center">Update Time</th>
    <th align="center">Info</th>
</tr>
</thead>
<tbody>
{{range $index, $task := .Tasks}}
<tr>
    <td>
        {{$key := index $.TaskKeys $index}} {{ KeyIntId $key }}
    </td>
    <td align="center">{{$task.Type}}</td>
    <td align="center">{{$task.Status}}</td>
    <td align="center">{{$task.Retries}}</td>
    <td align="center">{{$task.StartTime}}</td>
    <td align="center">{{$task.UpdatedAt}}</td>
    <td align="center">{{$task.Info}}</td>
</tr>

{{end}}
</tbody>
</table>
</body>
</html>
`

func ConsoleHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	ds, err := appwrap.NewDatastore(c)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if strings.HasSuffix(r.URL.Path, "/job") {
		id, _ := strconv.ParseInt(r.FormValue("id"), 10, 64)

		var tasks []JobTask
		var taskKeys []*appwrap.DatastoreKey
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
					taskKeys = makeTaskKeys(ds, job.Id, job.FirstTaskId, job.TaskCount)
				}
			default:
				jobKey := ds.NewKey(JobEntity, "", id, nil)
				if tk, err := ds.NewQuery(TaskEntity).Filter("Job =", jobKey).GetAll(&tasks); err != nil {
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

		funcMap := template.FuncMap{
			"KeyIntId": appwrap.KeyIntID,
		}

		t := template.New("main").Funcs(funcMap)
		t, err := t.Parse(head + css + js + jobPage)
		if err != nil {
			http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := t.Execute(w, struct {
			Id                             int64
			Tasks                          []JobTask
			TaskKeys                       []*appwrap.DatastoreKey
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

		jobList(ds, w, r, id)
	} else {
		jobList(ds, w, r, 0)
	}
}

func jobList(ds appwrap.Datastore, w http.ResponseWriter, r *http.Request, skipId int64) {
	c := appengine.NewContext(r)
	logger := appwrap.NewStackdriverLogging(c)
	q := ds.NewQuery(JobEntity).Order("-UpdatedAt")
	var jobs []JobInfo
	keys, err := q.GetAll(&jobs)
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
		if appwrap.KeyIntID(key) != skipId {
			job := annotatedJob{JobInfo: jobs[i]}
			if !jobs[i].StartTime.IsZero() {
				job.Duration = jobs[i].UpdatedAt.Sub(jobs[i].StartTime)
			}

			annotatedList = append(annotatedList, job)
		}
	}

	logger.Infof("%d jobs %d annotatedList", len(jobs), len(annotatedList))
	funcMap := template.FuncMap{
		"KeyIntId": appwrap.KeyIntID,
	}

	t := template.New("main").Funcs(funcMap)
	t, err = t.Parse(head + css + js + main)
	if err != nil {
		http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, struct {
		Jobs []annotatedJob
		Keys []*appwrap.DatastoreKey
	}{annotatedList, keys})
	if err != nil {
		http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
