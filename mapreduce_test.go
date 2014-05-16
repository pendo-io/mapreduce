package kyrie

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

type uniqueWordCount struct {
	InputReader
	KeyHandler
	OutputWriter
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
	r1, err := NewFileLineInputReader("test")
	if err != nil {
		log.Fatal(err)
	}

	r2, err := NewFileLineInputReader("test2")
	if err != nil {
		log.Fatal(err)
	}

	in := MultiInputReader{[]SingleInputReader{r1, r2}}

	out, err := NewFileLineOutputWriter("test.out")
	if err != nil {
		log.Fatal(err)
	}

	u := uniqueWordCount{}
	u.InputReader = in
	u.OutputWriter = out
	u.KeyHandler = StringKeyHandler{}

	job := MapReduceJob{
		MapReducePipeline: u,
		ReducerCount:      2,
	}

	Run(job)
}
