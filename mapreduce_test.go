package kyrie

import (
	"appengine"
	"appengine/aetest"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type uniqueWordCount struct {
	FileLineInputReader
	FileLineOutputWriter
	StringKeyHandler
	IntValueHandler
	IntermediateStorage
	SimpleTasks
}

type SimpleTasks struct {
	handler http.Handler
	done    chan bool
}

func (st SimpleTasks) PostTask(url string) error {
	fmt.Printf("posting task %s\n", url)
	if url == "/done" {
		st.done <- true
		return nil
	}

	req, _ := http.NewRequest("POST", url, nil)
	go func() {
		w := httptest.NewRecorder()
		st.handler.ServeHTTP(w, req)
		fmt.Printf("got %s\n", w)
	}()
	return nil
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
	u.IntermediateStorage = &FileIntermediateStorage{
		PathPattern: "mr-intermediate-%s",
		countPtr:    new(int32),
	}

	context, _ := aetest.NewContext(nil)
	defer context.Close()

	contextFn := func(r *http.Request) appengine.Context {
		return context
	}

	u.SimpleTasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", &u, contextFn),
		done:    make(chan bool),
	}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"test", "test2"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	err := Run(context, job)
	if err != nil {
		log.Fatal(err)
	}

	<-u.SimpleTasks.done
}
