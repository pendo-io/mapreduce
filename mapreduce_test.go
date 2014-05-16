package kyrie

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

type uniqueWordCount struct{}

func (uwc uniqueWordCount) Map(item interface{}) ([]MappedData, error) {
	line := item.(string)
	words := strings.Split(line, " ")
	result := make([]MappedData, 0, len(words))
	for _, word := range words {
		if len(word) > 0 {
			result = append(result, MappedData{word, 1})
		}
	}

	return result, nil
}

func (uwc uniqueWordCount) Reduce(key interface{}, values []interface{}) (result interface{}, err error) {
	return fmt.Sprintf("%s: %d", key, len(values)), nil
}

func TestSomething(t *testing.T) {
	in, err := NewFileLineInputReader("test")
	if err != nil {
		log.Fatal(err)
	}

	out, err := NewFileLineOutputWriter("test.out")
	if err != nil {
		log.Fatal(err)
	}

	pipe := MapReducePipeline{
		Input:   in,
		Output:  out,
		Mapper:  uniqueWordCount{},
		Reducer: uniqueWordCount{},
		Compare: StringComparator{},
	}
	pipe.Run()
}
