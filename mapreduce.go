package mapreduce

import (
	"appengine"
	"appengine/datastore"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type MappedData struct {
	Key   interface{}
	Value interface{}
}

type Mapper interface {
	Map(item interface{}) ([]MappedData, error)
}

type Reducer interface {
	Reduce(key interface{}, values []interface{}) (result interface{}, err error)
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
}

type mappedDataList struct {
	data    []MappedData
	compare KeyHandler
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

// this should be a priority queue instead of continually resorting
type shardMappedDataList struct {
	feeders []IntermediateStorageIterator
	data    []MappedData
	compare KeyHandler
}

func (a shardMappedDataList) Len() int           { return len(a.data) }
func (a shardMappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }
func (a shardMappedDataList) Swap(i, j int) {
	a.data[i], a.data[j] = a.data[j], a.data[i]
	a.feeders[i], a.feeders[j] = a.feeders[j], a.feeders[i]
}

func (s *shardMappedDataList) next() (MappedData, error) {
	sort.Sort(s)
	item := s.data[0]
	if len(s.data) != len(s.feeders) {
		panic("ACK")
	}

	if newItem, exists, err := s.feeders[0].Next(); err != nil {
		return MappedData{}, err
	} else if exists {
		s.data[0] = newItem
	} else if len(s.data) == 1 {
		s.data = s.data[0:0]
		s.feeders = s.feeders[0:0]
	} else {
		last := len(s.data) - 1
		s.data[0] = s.data[last]
		s.feeders[0] = s.feeders[last]
		s.data = s.data[0:last]
		s.feeders = s.feeders[0:last]
	}

	if len(s.data) != len(s.feeders) {
		panic("ACK")
	}

	return item, nil
}

type reduceResult struct {
	error
	storageNames []string
}

func Run(c appengine.Context, job MapReduceJob) (int64, error) {
	inputs, err := job.Inputs.Split()
	if err != nil {
		return 0, err
	}

	reducerCount := job.Outputs.WriterCount()

	writers, err := job.Outputs.Writers(c)
	if err != nil {
		return 0, err
	}

	writerNames := make([]string, len(writers))
	for i := range writers {
		writerNames[i] = writers[i].ToName()
	}

	jobKey, err := createJob(c, job.UrlPrefix, writerNames, job.OnCompleteUrl)
	if err != nil {
		return 0, err
	}

	taskKeys := make([]*datastore.Key, len(inputs))
	tasks := make([]JobTask, len(inputs))
	firstId, _, err := datastore.AllocateIDs(c, TaskEntity, jobKey, len(inputs))
	if err != nil {
		return 0, err
	}

	for i, input := range inputs {
		taskKeys[i] = datastore.NewKey(c, TaskEntity, "", firstId, jobKey)
		firstId++

		url := fmt.Sprintf("%s/map?taskKey=%s;reader=%s;shards=%d",
			job.UrlPrefix, taskKeys[i].Encode(), input.ToName(),
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

type handler struct {
	pipeline   MapReducePipeline
	baseUrl    string
	getContext func(r *http.Request) appengine.Context
}

func MapReduceHandler(baseUrl string, pipeline MapReducePipeline,
	getContext func(r *http.Request) appengine.Context) http.Handler {
	return handler{pipeline, baseUrl, getContext}
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	} else {
		http.Error(w, "unknown request uel", http.StatusNotFound)
		return
	}
}
