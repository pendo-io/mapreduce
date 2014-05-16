package kyrie

import (
	"sort"
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
	feeders []ShardFeeder
	data    []MappedData
	compare KeyHandler
}

func (a shardMappedDataList) Len() int           { return len(a.data) }
func (a shardMappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }
func (a shardMappedDataList) Swap(i, j int) {
	a.data[i], a.data[j] = a.data[j], a.data[i]
	a.feeders[i], a.feeders[j] = a.feeders[j], a.feeders[i]
}

func (s *shardMappedDataList) next() MappedData {
	sort.Sort(s)
	item := s.data[0]
	if len(s.data) != len(s.feeders) {
		panic("ACK")
	}

	if newItem, exists := s.feeders[0].next(); exists {
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

	return item
}

type ShardFeeder struct {
	data      []MappedData
	nextIndex int
	name      int
}

func (sf *ShardFeeder) next() (MappedData, bool) {
	if sf.nextIndex >= len(sf.data) {
		return MappedData{}, false
	}

	sf.nextIndex++
	return sf.data[sf.nextIndex-1], true
}

type reduceResult struct {
	error
	data []mappedDataList
}

func Run(job MapReduceJob) error {
	inputs, err := job.Inputs.Split()
	if err != nil {
		return err
	}

	reducerCount := job.Outputs.WriterCount()

	ch := make(chan reduceResult)

	for _, input := range inputs {
		go MapTask(job, input.ToName(), reducerCount, ch)
	}

	// we have one set for each input, each set has ReducerCount data sets in it
	// (each of which is already sorted)
	shardSets := make([][]mappedDataList, 0, len(inputs))

	jobs := len(inputs)
	for jobs > 0 {
		result := <-ch
		if result.error != nil {
			return result.error
		}

		jobs--
		shardSets = append(shardSets, result.data)
	}

	close(ch)

	results := make(chan error)
	writers, err := job.Outputs.Writers()
	if err != nil {
		return err
	}

	for shard, writer := range writers {
		shards := make([][]MappedData, 0, len(inputs))

		for i := range inputs {
			if len(shardSets[i][shard].data) > 0 {
				shards = append(shards, shardSets[i][shard].data)
			}
		}

		if len(shards) > 0 {
			go ReduceTask(job, writer.ToName(), shards, results)
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

func MapTask(mr MapReducePipeline, readerName string, shardCount int, ch chan reduceResult) {
	reader, err := mr.ReaderFromName(readerName)
	if err != nil {
		ch <- reduceResult{err, nil}
		return
	}

	MapperFunc(mr, reader, shardCount, ch)
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

	for i := range dataSets {
		sort.Sort(dataSets[i])
	}

	ch <- reduceResult{nil, dataSets}
}

func ReduceTask(mr MapReducePipeline, writerName string, inputs [][]MappedData, resultChannel chan error) {
	writer, err := mr.WriterFromName(writerName)
	if err != nil {
		resultChannel <- err
		return
	}

	ReduceFunc(mr, writer, inputs, resultChannel)
}

func ReduceFunc(mr MapReducePipeline, writer SingleOutputWriter, inputs [][]MappedData, resultChannel chan error) {
	inputCount := len(inputs)

	items := shardMappedDataList{
		feeders: make([]ShardFeeder, 0, inputCount),
		data:    make([]MappedData, 0, inputCount),
		compare: mr,
	}

	for input := range inputs {
		if len(inputs[input]) > 0 {
			items.feeders = append(items.feeders, ShardFeeder{inputs[input], 1, input})
			items.data = append(items.data, inputs[input][0])
		}
	}

	if len(items.data) == 0 {
		resultChannel <- nil
		return
	}

	first := items.next()
	key := first.Key
	values := make([]interface{}, 1)
	values[0] = first.Value

	for len(items.data) > 0 {
		item := items.next()

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
