package kyrie

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
}

type MapReduceJob struct {
	MapReducePipeline
	Inputs  InputReader
	Outputs OutputWriter
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

func Run(job MapReduceJob) error {
	inputs, err := job.Inputs.Split()
	if err != nil {
		return err
	}

	reducerCount := job.Outputs.WriterCount()

	ch := make(chan reduceResult)

	for _, input := range inputs {
		request, _ := http.NewRequest("POST",
			fmt.Sprintf("http://foo.com?reader=%s;shards=%d",
				input.ToName(), reducerCount), nil)
		go MapTask(job, request, ch)
	}

	// we have one set for each input, each set has ReducerCount data sets in it
	// (each of which is already sorted)
	storageNames := make([][]string, 0, len(inputs))

	jobs := len(inputs)
	for jobs > 0 {
		result := <-ch
		if result.error != nil {
			return result.error
		}

		jobs--
		storageNames = append(storageNames, result.storageNames)
	}

	close(ch)

	results := make(chan error)
	writers, err := job.Outputs.Writers()
	if err != nil {
		return err
	}

	for shard, writer := range writers {
		shards := make([]string, 0, len(inputs))

		for i := range inputs {
			shards = append(shards, storageNames[i][shard])
		}

		if len(shards) > 0 {
			shardSet, _ := json.Marshal(shards)
			shardParam := url.QueryEscape(string(shardSet))
			request, _ := http.NewRequest("POST",
				fmt.Sprintf("http://foo.com?writer=%s;shards=%s",
					writer.ToName(), shardParam), nil)

			go ReduceTask(job, request, results)
		}
	}

	jobs = len(writers)
	var finalErr error = nil
	for jobs > 0 {
		err := <-results
		if err != nil && finalErr == nil {
			finalErr = err
		}

		jobs--
	}

	return finalErr
}

func MapTask(mr MapReducePipeline, r *http.Request, ch chan reduceResult) {
	readerName := r.FormValue("reader")
	if readerName == "" {
		//http.Error(w, "reader parameter is required", http.StatusBadRequest)
		ch <- reduceResult{fmt.Errorf("reader parameter required"), nil}
		return
	}

	var shardCount int64
	var err error
	if shardStr := r.FormValue("shards"); shardStr == "" {
		//http.Error(w, "shards parameter is required", http.StatusBadRequest)
		ch <- reduceResult{fmt.Errorf("shrads parameter required"), nil}
		return
	} else if shardCount, err = strconv.ParseInt(shardStr, 10, 32); err != nil {
		//http.Error(w, "bad shards value", http.StatusBadRequest)
		ch <- reduceResult{fmt.Errorf("bad shards"), nil}
		return
	}

	reader, err := mr.ReaderFromName(readerName)
	if err != nil {
		ch <- reduceResult{err, nil}
		return
	}

	MapperFunc(mr, reader, int(shardCount), ch)
}

func MapperFunc(mr MapReducePipeline, reader SingleInputReader, shardCount int, ch chan reduceResult) {
	dataSets := make([]mappedDataList, shardCount)
	for i := range dataSets {
		dataSets[i] = mappedDataList{data: make([]MappedData, 0), compare: mr}
	}

	for item, err := reader.Next(); item != nil && err == nil; item, err = reader.Next() {
		itemList, err := mr.Map(item)

		if err != nil {
			ch <- reduceResult{err, nil}
			return
		}

		for _, item := range itemList {
			shard := mr.Shard(item.Key, shardCount)
			dataSets[shard].data = append(dataSets[shard].data, item)
		}
	}

	names := make([]string, len(dataSets))
	for i := range dataSets {
		var err error

		sort.Sort(dataSets[i])
		names[i], err = mr.Store(dataSets[i].data, mr)
		if err != nil {
			ch <- reduceResult{err, nil}
			return
		}
	}

	ch <- reduceResult{nil, names}
}

func ReduceTask(mr MapReducePipeline, r *http.Request, resultChannel chan error) {
	writerName := r.FormValue("writer")
	if writerName == "" {
		//http.Error(w, "writer parameter is required", http.StatusBadRequest)
		resultChannel <- fmt.Errorf("writer parameter required")
		return
	}

	var shardNames []string
	if shardParam := r.FormValue("shards"); shardParam == "" {
		//http.Error(w, "shards parameter is required", http.StatusBadRequest)
		resultChannel <- fmt.Errorf("shards parameter required")
		return
	} else if shardJson, err := url.QueryUnescape(shardParam); err != nil {
		//http.Error(w, "cannot urldecode shards", http.StatusBadRequest)
		resultChannel <- fmt.Errorf("cannot urldecode shards")
		return
	} else if err := json.Unmarshal([]byte(shardJson), &shardNames); err != nil {
		//http.Error(w, "cannot unmarshal shard names", http.StatusBadRequest)
		resultChannel <- fmt.Errorf("cannot unmarshal shard names nil}")
		return
	}

	writer, err := mr.WriterFromName(writerName)
	if err != nil {
		resultChannel <- err
		return
	}

	ReduceFunc(mr, writer, shardNames, resultChannel)
}

func ReduceFunc(mr MapReducePipeline, writer SingleOutputWriter, shardNames []string, resultChannel chan error) {
	inputCount := len(shardNames)

	items := shardMappedDataList{
		feeders: make([]IntermediateStorageIterator, 0, inputCount),
		data:    make([]MappedData, 0, inputCount),
		compare: mr,
	}

	for _, shardName := range shardNames {
		iterator, err := mr.Iterator(shardName, mr)
		if err != nil {
			resultChannel <- err
			return
		}

		firstItem, exists, err := iterator.Next()
		if err != nil {
			resultChannel <- err
			return
		} else if !exists {
			continue
		}

		items.feeders = append(items.feeders, iterator)
		items.data = append(items.data, firstItem)
	}

	if len(items.data) == 0 {
		resultChannel <- nil
		return
	}

	values := make([]interface{}, 1)
	var key interface{}

	if first, err := items.next(); err != nil {
		resultChannel <- err
		return
	} else {
		key = first.Key
		values[0] = first.Value
	}

	for len(items.data) > 0 {
		item, err := items.next()
		if err != nil {
			resultChannel <- err
			return
		}

		if mr.Equal(key, item.Key) {
			values = append(values, item.Value)
			continue
		}

		if result, err := mr.Reduce(key, values); err != nil {
			resultChannel <- err
			return
		} else if err := writer.Write(result); err != nil {
			resultChannel <- err
			return
		}

		key = item.Key
		values = values[0:1]
		values[0] = item.Value
	}

	if result, err := mr.Reduce(key, values); err != nil {
		resultChannel <- err
		return
	} else if err := writer.Write(result); err != nil {
		resultChannel <- err
		return
	}

	writer.Close()

	resultChannel <- nil
}
