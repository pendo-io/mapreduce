package mapreduce

import (
	"appengine"
	"fmt"
	ck "gopkg.in/check.v1"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	if strings.HasPrefix(url, "/done") {
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

func (mrt *MapreduceTests) TestWordCount(c *ck.C) {
	u := testUniqueWordCount{}

	u.SimpleTasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", &u, mrt.ContextFn),
		done:    make(chan string),
	}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"testdata/pandp-1", "testdata/pandp-2", "testdata/pandp-3", "testdata/pandp-4", "testdata/pandp-5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	_, err := Run(mrt.Context, job)
	c.Assert(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done
	c.Check(strings.Index(resultUrl, "status=done"), ck.Equals, 6)
}

type testMapPanic struct {
	testUniqueWordCount
}

func (tmp testMapPanic) Map(item interface{}, status StatusUpdateFunc) ([]MappedData, error) {
	mapped, err := tmp.testUniqueWordCount.Map(item, status)
	for _, data := range mapped {
		// this occurs exactly once, in input 2
		if data.Key == "enumeration" {
			panic("I prefer not to enumerate")
		}
	}

	return mapped, err
}

func (mrt *MapreduceTests) TestMapPanic(c *ck.C) {
	u := testMapPanic{}

	u.SimpleTasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", &u, mrt.ContextFn),
		done:    make(chan string),
	}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"testdata/pandp-1", "testdata/pandp-2", "testdata/pandp-3", "testdata/pandp-4", "testdata/pandp-5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed map: I prefer not to enumerate")
}

type testReducePanic struct {
	testUniqueWordCount
}

func (trp testReducePanic) Reduce(key interface{}, values []interface{}, status StatusUpdateFunc) (result interface{}, err error) {
	if key.(string) == "enumeration" {
		panic("Reduce panic")
	}

	return trp.testUniqueWordCount.Reduce(key, values, status)
}

func (mrt *MapreduceTests) TestReducePanic(c *ck.C) {
	u := testReducePanic{}

	u.SimpleTasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", &u, mrt.ContextFn),
		done:    make(chan string),
	}

	job := MapReduceJob{
		MapReducePipeline: u,
		Inputs:            FileLineInputReader{[]string{"testdata/pandp-1", "testdata/pandp-2", "testdata/pandp-3", "testdata/pandp-4", "testdata/pandp-5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed map: Reduce panic")
}
