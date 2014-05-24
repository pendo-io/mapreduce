package mapreduce

import (
	"appengine"
	"appengine/aetest"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type testUniqueWordCount struct {
	FileLineInputReader
	FileLineOutputWriter
	StringKeyHandler
	IntValueHandler
	BlobIntermediateStorage
	SimpleTasks

	lineCount int
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

func (st SimpleTasks) PostStatus(c appengine.Context, url string) error {
	return st.PostTask(c, url)
}

func (uwc testUniqueWordCount) Map(item interface{}, status StatusUpdateFunc) ([]MappedData, error) {
	line := item.(string)
	words := strings.Split(line, " ")
	result := make([]MappedData, 0, len(words))
	count := 0
	for _, word := range words {
		if len(word) > 0 {
			result = append(result, MappedData{word, 1})
		}
		count++
	}

	uwc.lineCount++
	if uwc.lineCount%2500 == 0 {
		status("line %d", uwc.lineCount)
	}

	return result, nil
}

func (uwc testUniqueWordCount) Reduce(key interface{}, values []interface{}, status StatusUpdateFunc) (result interface{}, err error) {
	return fmt.Sprintf("%s: %d", key, len(values)), nil
}

func TestSomething(t *testing.T) {
	u := testUniqueWordCount{}

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
		Inputs:            FileLineInputReader{[]string{"testdata/pandp-1", "testdata/pandp-2", "testdata/pandp-3", "testdata/pandp-4", "testdata/pandp-5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	_, err := Run(context, job)
	if err != nil {
		t.Logf("mapreduce failed to run: %s", err)
		t.Fail()
	} else {
		resultUrl := <-u.SimpleTasks.done
		//t.Logf("got result: %s\n", resultUrl)
		if strings.Index(resultUrl, "status=done") >= 0 {
			t.Fail()
		}
	}
}
