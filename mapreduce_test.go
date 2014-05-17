package kyrie

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

type uniqueWordCount struct {
	FileLineInputReader
	FileLineOutputWriter
	StringKeyHandler
	StringValueHandler
}

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
	u := uniqueWordCount{}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"test", "test2"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
	}

	err := Run(job)
	if err != nil {
		log.Fatal(err)
	}
}
