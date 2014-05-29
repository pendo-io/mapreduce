// Copyright 2014 pendo.io
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapreduce

import (
	"appengine"
	"fmt"
	ck "gopkg.in/check.v1"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
)

type testUniqueWordCount struct {
	FileLineInputReader
	FileLineOutputWriter
	StringKeyHandler
	IntValueHandler
	MemoryIntermediateStorage
	SimpleTasks

	lineCount int
}

type SimpleTasks struct {
	handler http.Handler
	done    chan string
	group   sync.WaitGroup
}

func (st *SimpleTasks) PostTask(c appengine.Context, url string) error {
	if strings.HasPrefix(url, "/done") {
		st.done <- url
		return nil
	}

	req, _ := http.NewRequest("POST", url, nil)
	st.group.Add(1)
	go func() {
		defer st.group.Done()
		w := httptest.NewRecorder()
		st.handler.ServeHTTP(w, req)
		if w.Code != 200 {
			fmt.Printf("Got bad response code %s for url %s\n", w.Code, url)
		}
	}()

	return nil
}

func (st *SimpleTasks) PostStatus(c appengine.Context, url string) error {
	return st.PostTask(c, url)
}

func (st *SimpleTasks) gather() {
	st.group.Wait()
}

func (mrt *MapreduceTests) setup(pipe MapReducePipeline, tasks *SimpleTasks) MapReduceJob {
	*tasks = SimpleTasks{
		handler: MapReduceHandler("/mr/test", pipe, mrt.ContextFn),
		done:    make(chan string),
	}

	job := MapReduceJob{
		MapReducePipeline: pipe,
		Inputs:            FileLineInputReader{[]string{"testdata/pandp-1", "testdata/pandp-2", "testdata/pandp-3", "testdata/pandp-4", "testdata/pandp-5"}},
		Outputs:           FileLineOutputWriter{[]string{"test1.out", "test2.out"}},
		UrlPrefix:         "/mr/test",
		OnCompleteUrl:     "/done",
	}

	return job
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
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

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
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed task: I prefer not to enumerate")
}

type testMapError struct {
	testUniqueWordCount
	fatal bool
}

func (tmp testMapError) Map(item interface{}, status StatusUpdateFunc) ([]MappedData, error) {
	mapped, err := tmp.testUniqueWordCount.Map(item, status)
	for _, data := range mapped {
		// this occurs exactly once, in input 2
		if data.Key == "enumeration" {
			err := fmt.Errorf("map had an error")
			if tmp.fatal {
				err = FatalError{err}
			}
			print("----- error here", "\n")
			return nil, err
		}
	}

	return mapped, err
}

func (mrt *MapreduceTests) TestMapError(c *ck.C) {
	u := testMapError{}
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	print("result ", resultUrl, "\n")
	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "error retrying: maxium retries exceeded (task failed due to: map had an error)")
}

func (mrt *MapreduceTests) TestMapFatal(c *ck.C) {
	u := testMapError{fatal: true}
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	print("result ", resultUrl, "\n")
	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed task: map had an error")
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
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed task: Reduce panic")
}

type testReduceError struct {
	testUniqueWordCount
	fatal            bool
	count            int
	succeedThreshold int
}

func (trp *testReduceError) Reduce(key interface{}, values []interface{}, status StatusUpdateFunc) (result interface{}, err error) {
	trp.count++

	if (trp.count < trp.succeedThreshold || trp.succeedThreshold == 0) && key.(string) == "enumeration" {
		err := fmt.Errorf("reduce had an error")

		if trp.fatal {
			err = FatalError{err}
		}
		return nil, err
	}

	return trp.testUniqueWordCount.Reduce(key, values, status)
}

func (mrt *MapreduceTests) TestReduceError(c *ck.C) {
	u := testReduceError{}
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "error retrying: maxium retries exceeded (task failed due to: reduce had an error)")

	// see if we handle retries properly
	v := testReduceError{succeedThreshold: u.count / 2}
	job = mrt.setup(&v, &v.SimpleTasks)
	defer v.SimpleTasks.gather()

	_, err = Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl = <-v.SimpleTasks.done
	c.Check(strings.Index(resultUrl, "status=done"), ck.Equals, 6)
}

func (mrt *MapreduceTests) TestReduceFatal(c *ck.C) {
	u := testReduceError{fatal: true}
	job := mrt.setup(&u, &u.SimpleTasks)
	defer u.SimpleTasks.gather()

	_, err := Run(mrt.Context, job)
	c.Check(err, ck.Equals, nil)

	resultUrl := <-u.SimpleTasks.done

	url, err := url.Parse(resultUrl)
	c.Check(err, ck.IsNil)
	fields := url.Query()
	c.Check(err, ck.IsNil)

	c.Check(fields["status"][0], ck.Equals, "error")
	c.Check(fields["error"][0], ck.Equals, "failed task: reduce had an error")
}
