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

type MapReducePipeline interface {
	InputReader
	Mapper
	Reducer
	OutputWriter
	Comparator
}

type MapReduceJob struct {
	Pipeline MapReducePipeline
}

type mappedDataList struct {
	data    []MappedData
	compare Comparator
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

type reduceResult struct {
	error
	data []MappedData
}

func Run(mr MapReducePipeline) error {
	inputs, err := mr.Split()
	if err != nil {
		return err
	}

	data := mappedDataList{data: make([]MappedData, 0), compare: mr}

	ch := make(chan reduceResult)

	for _, input := range inputs {
		go MapTask(input, mr, ch)
	}

	jobs := len(inputs)
	for jobs > 0 {
		result := <-ch
		if result.error != nil {
			return result.error
		} else if result.data == nil {
			jobs--
		}

		data.data = append(data.data, result.data...)
	}

	close(ch)

	if len(data.data) == 0 {
		return nil
	}

	sort.Sort(data)

	key := data.data[0].Key
	values := make([]interface{}, 1)
	values[0] = data.data[0].Value
	for i := 1; i < len(data.data); i++ {
		if mr.Equal(key, data.data[i].Key) {
			values = append(values, data.data[i].Value)
			continue
		}

		if result, err := mr.Reduce(key, values); err != nil {
			return err
		} else if err := mr.Write(result); err != nil {
			return err
		}

		key = data.data[i].Key
		values = values[0:1]
		values[0] = data.data[i].Value
	}

	if result, err := mr.Reduce(key, values); err != nil {
		return err
	} else if err := mr.Write(result); err != nil {
		return err
	}

	return nil
}

func MapTask(reader SingleInputReader, mapper Mapper, ch chan reduceResult) {

	for item, err := reader.Next(); item != nil && err == nil; item, err = reader.Next() {
		var result reduceResult
		result.data, result.error = mapper.Map(item)
		ch <- result

		if result.error != nil {
			break
		}
	}

	ch <- reduceResult{nil, nil}
}
