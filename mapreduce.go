package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"container/heap"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type MappedData struct {
	Key   interface{}
	Value interface{}
}

type StatusUpdateFunc func(format string, paramList ...interface{})

type Mapper interface {
	Map(item interface{}, statusUpdate StatusUpdateFunc) ([]MappedData, error)
}

type Reducer interface {
	Reduce(key interface{}, values []interface{}, statusUpdate StatusUpdateFunc) (result interface{}, err error)
}

type Sharder interface {
	ShardCount() int
	ShardKey(key interface{}) int
}

type MapReducePipeline interface {
	InputReader
	Mapper
	Reducer
	OutputWriter
	KeyHandler
	ValueHandler
	IntermediateStorage
	TaskInterface
}

type MapReduceJob struct {
	MapReducePipeline
	Inputs        InputReader
	Outputs       OutputWriter
	UrlPrefix     string
	OnCompleteUrl string
	RetryCount    int
}

type mappedDataList struct {
	data    []MappedData
	compare KeyHandler
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

type mappedDataMergeItem struct {
	iterator IntermediateStorageIterator
	datum    MappedData
}

type mappedDataMerger struct {
	items   []mappedDataMergeItem
	compare KeyHandler
	inited  bool
}

func (a *mappedDataMerger) Len() int { return len(a.items) }
func (a *mappedDataMerger) Less(i, j int) bool {
	return a.compare.Less(a.items[i].datum.Key, a.items[j].datum.Key)
}
func (a *mappedDataMerger) Swap(i, j int)      { a.items[i], a.items[j] = a.items[j], a.items[i] }
func (a *mappedDataMerger) Push(x interface{}) { a.items = append(a.items, x.(mappedDataMergeItem)) }
func (a *mappedDataMerger) Pop() interface{} {
	x := a.items[len(a.items)-1]
	a.items = a.items[0 : len(a.items)-1]
	return x
}

func (s *mappedDataMerger) next() (MappedData, error) {
	if !s.inited {
		heap.Init(s)
		s.inited = true
	}

	item := heap.Pop(s).(mappedDataMergeItem)

	if newItem, exists, err := item.iterator.Next(); err != nil {
		return MappedData{}, err
	} else if exists {
		heap.Push(s, mappedDataMergeItem{item.iterator, newItem})
	}

	return item.datum, nil
}

func Run(c appengine.Context, job MapReduceJob) (int64, error) {
	readerNames, err := job.Inputs.ReaderNames()
	if err != nil {
		return 0, err
	} else if len(readerNames) == 0 {
		return 0, fmt.Errorf("no input readers")
	}

	if job.RetryCount == 0 {
		// default
		job.RetryCount = 3
	}

	writerNames, err := job.Outputs.WriterNames(c)
	if err != nil {
		return 0, err
	} else if len(writerNames) == 0 {
		return 0, fmt.Errorf("no output writers")
	}

	reducerCount := len(writerNames)

	jobKey, err := createJob(c, job.UrlPrefix, writerNames, job.OnCompleteUrl)
	if err != nil {
		return 0, err
	}

	taskKeys := make([]*datastore.Key, len(readerNames))
	tasks := make([]JobTask, len(readerNames))
	firstId, _, err := datastore.AllocateIDs(c, TaskEntity, jobKey, len(readerNames))
	if err != nil {
		return 0, err
	}

	for i, readerName := range readerNames {
		taskKeys[i] = datastore.NewKey(c, TaskEntity, "", firstId, jobKey)
		firstId++

		url := fmt.Sprintf("%s/map?taskKey=%s;reader=%s;shards=%d",
			job.UrlPrefix, taskKeys[i].Encode(), readerName,
			reducerCount)

		tasks[i] = JobTask{
			Status:   TaskStatusPending,
			RunCount: 0,
			Url:      url,
			Type:     TaskTypeMap,
		}
	}

	if err := createTasks(c, jobKey, taskKeys, tasks, StageMapping); err != nil {
		return 0, err
	}

	for i := range tasks {
		if err := job.PostTask(c, tasks[i].Url); err != nil {
			return 0, err
		}
	}

	return jobKey.IntID(), nil
}

type urlHandler struct {
	pipeline   MapReducePipeline
	baseUrl    string
	getContext func(r *http.Request) appengine.Context
}

func MapReduceHandler(baseUrl string, pipeline MapReducePipeline,
	getContext func(r *http.Request) appengine.Context) http.Handler {
	return urlHandler{pipeline, baseUrl, getContext}
}

func (h urlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var taskKey *datastore.Key
	var err error

	if taskKeyStr := r.FormValue("taskKey"); taskKeyStr == "" {
		http.Error(w, "taskKey parameter required", http.StatusBadRequest)
		return
	} else if taskKey, err = datastore.DecodeKey(taskKeyStr); err != nil {
		http.Error(w, fmt.Sprintf("invalid taskKey: %s", err.Error()),
			http.StatusBadRequest)
		return
	}

	c := h.getContext(r)

	if strings.HasSuffix(r.URL.Path, "/reduce") {
		ReduceTask(c, h.baseUrl, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/reducecomplete") {
		ReduceCompleteTask(c, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/map") {
		MapTask(c, h.baseUrl, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/mapcomplete") {
		MapCompleteTask(c, h.pipeline, taskKey, r)
	} else if strings.HasSuffix(r.URL.Path, "/mapstatus") ||
		strings.HasSuffix(r.URL.Path, "/reducestatus") {

		updateTask(c, taskKey, "", r.FormValue("msg"), nil)
	} else {
		http.Error(w, "unknown request url", http.StatusNotFound)
		return
	}
}

func makeStatusUpdateFunc(c appengine.Context, pipeline MapReducePipeline, urlStr string, taskKey string) StatusUpdateFunc {
	return func(format string, paramList ...interface{}) {
		pipeline.PostStatus(c, fmt.Sprintf("%s?taskKey=%s;msg=%s", urlStr, taskKey,
			url.QueryEscape(fmt.Sprintf(format, paramList...))))
	}
}
