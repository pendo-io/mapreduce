package mapreduce

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
	done    chan string
}

func (st SimpleTasks) PostTask(c appengine.Context, url string) error {
	if strings.Index(url, "/done") >= 0 {
		st.done <- url
		return nil
	}

	req, _ := http.NewRequest("POST", url, nil)
	go func() {
		w := httptest.NewRecorder()
		st.handler.ServeHTTP(w, req)
		if w.Code != 200 {
			fmt.Printf("Got bad response code %s for url %s\n", w.Code, url)
		}
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
	u.IntermediateStorage = &BlobIntermediateStorage{}

	context, _ := aetest.NewContext(nil)
	defer context.Close()

	contextFn := func(r *http.Request) appengine.Context {
		return context
	}

	u.SimpleTasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", &u, contextFn),
		done:    make(chan string),
	}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"p1", "p2", "p3", "p4", "p5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	err := Run(context, job)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("got: %s\n", <-u.SimpleTasks.done)
}
