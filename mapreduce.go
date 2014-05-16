package kyrie

type Mapper interface {
	Map(item interface{}) (key interface{}, value interface{}, err error)
}

type Reducer interface {
	Reduce(key interface{}, values []interface{}) (result interface{}, err error)
}

type MapReducePipeline struct {
	input   InputReader
	mapper  Mapper
	reducer Reducer
	output  OutputWriter
}
