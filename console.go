package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"html/template"
	"net/http"
	"strconv"
	"strings"
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
    <th align="center">Updated Time</th>
    <th align="center">Tasks Running</th>
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
    <td>{{$job.UpdatedAt}}</td>
    <td align="center">{{$job.TasksRunning}}</td>
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

<table>
<tr>
    <th align="center">Id</th>
    <th align="center">Type</th>
    <th align="center">Status</th>
    <th align="center">Retries</th>
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
    <td align="center">{{$task.UpdatedAt}}</td>
    <td align="center">{{$task.Info}}</td>
</tr>

{{end}}

</table>

`

func ConsoleHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if strings.HasSuffix(r.URL.Path, "/job") {
		id, _ := strconv.ParseInt(r.FormValue("id"), 10, 64)
		jobKey := datastore.NewKey(c, JobEntity, "", id, nil)

		q := datastore.NewQuery(TaskEntity).Ancestor(jobKey)
		var tasks []JobTask
		taskKeys, err := q.GetAll(c, &tasks)
		if err != nil {
			http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		t := template.New("main")
		t, _ = t.Parse(jobPage)
		err = t.Execute(w, struct {
			Id       int64
			Tasks    []JobTask
			TaskKeys []*datastore.Key
		}{id, tasks, taskKeys})
		if err != nil {
			http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else if strings.HasSuffix(r.URL.Path, "/delete") {
		id, _ := strconv.ParseInt(r.FormValue("id"), 10, 64)

		if err := RemoveJob(c, id); err != nil {
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

	if skipId != 0 {
		for i, key := range keys {
			if key.IntID() == skipId {
				jobs = append(jobs[0:i], jobs[i+1:len(jobs)-1]...)
				break
			}
		}
	}

	t := template.New("main")
	t, _ = t.Parse(main)
	err = t.Execute(w, struct {
		Jobs []JobInfo
		Keys []*datastore.Key
	}{jobs, keys})
	if err != nil {
		http.Error(w, "Internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}
}
