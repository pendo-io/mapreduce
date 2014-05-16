package kyrie

import (
	"fmt"
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

type MapReducePipeline struct {
	Input   InputReader
	Mapper  Mapper
	Reducer Reducer
	Output  OutputWriter
	Compare Comparator
}

type mappedDataList struct {
	data    []MappedData
	compare Comparator
}

func (a mappedDataList) Len() int           { return len(a.data) }
func (a mappedDataList) Swap(i, j int)      { a.data[i], a.data[j] = a.data[j], a.data[i] }
func (a mappedDataList) Less(i, j int) bool { return a.compare.Less(a.data[i].Key, a.data[j].Key) }

func (mr MapReducePipeline) String() string {
	return fmt.Sprintf("MapReducePipeline{input=%s, output=%s}", mr.Input, mr.Output)
}

func (mr MapReducePipeline) Run() error {
	inputs, err := mr.Input.Split()
	if err != nil {
		return err
	}

	data := mappedDataList{data: make([]MappedData, 0), compare: mr.Compare}

	for _, input := range inputs {
		var err error
		var item interface{}

		for item, err = input.Next(); item != nil && err == nil; item, err = input.Next() {
			result, err := mr.Mapper.Map(item)
			if err != nil {
				return err
			}

			data.data = append(data.data, result...)
		}

		if err != nil {
			return err
		}
	}

	if len(data.data) == 0 {
		return nil
	}

	sort.Sort(data)

	key := data.data[0].Key
	values := make([]interface{}, 1)
	values[0] = data.data[0].Value
	for i := 1; i < len(data.data); i++ {
		if mr.Compare.Equal(key, data.data[i].Key) {
			values = append(values, data.data[i].Value)
			continue
		}

		if result, err := mr.Reducer.Reduce(key, values); err != nil {
			return err
		} else if err := mr.Output.Write(result); err != nil {
			return err
		}

		key = data.data[i].Key
		values = values[0:1]
		values[0] = data.data[i].Value
	}

	if result, err := mr.Reducer.Reduce(key, values); err != nil {
		return err
	} else if err := mr.Output.Write(result); err != nil {
		return err
	}

	return nil
}
