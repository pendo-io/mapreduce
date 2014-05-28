package mapreduce

import (
	"appengine"
	"appengine/aetest"
	"gopkg.in/check.v1"
	"net/http"
	"testing"
)

type MapreduceTests struct {
	Context aetest.Context
}

func (mrt *MapreduceTests) ContextFn(*http.Request) appengine.Context {
	return mrt.Context
}

var _ = check.Suite(&MapreduceTests{})

func TestMapReduce(t *testing.T) { check.TestingT(t) }

func (s *MapreduceTests) SetUpSuite(c *check.C) {
	if context, err := aetest.NewContext(nil); err != nil {
		c.Fatal(err)
	} else {
		s.Context = context
	}

}

func (s *MapreduceTests) TearDownSuite(c *check.C) {
	s.Context.Close()
}
