package kyrie

type Comparator interface {
	Less(a, b interface{}) bool
	Equal(a, b interface{}) bool
}

type StringComparator struct{}

func (s StringComparator) Less(a, b interface{}) bool {
	return a.(string) < b.(string)
}

func (s StringComparator) Equal(a, b interface{}) bool {
	return a.(string) == b.(string)
}
