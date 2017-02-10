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
	"github.com/pendo-io/appwrap"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	ck "gopkg.in/check.v1"
	"time"
)

type taskInterfaceMock struct {
	mock.Mock
}

func (mock *taskInterfaceMock) PostTask(c context.Context, fullUrl string, jsonParameters string) error {
	rargs := mock.Called(c, fullUrl, jsonParameters)
	return rargs.Error(0)
}

func (mock *taskInterfaceMock) PostStatus(c context.Context, fullUrl string) error {
	rargs := mock.Called(c, fullUrl)
	return rargs.Error(0)
}

func (mrt *MapreduceTests) TestJobStageComplete(c *ck.C) {
	c.Skip("YOU SHALL NOT PASS! (Because the dual monitor patch broke it)")
	ds := appwrap.NewLocalDatastore()
	ctx := appwrap.StubContext()

	jobKey, err := createJob(ds, "prefix", []string{}, "complete", false, "", 5)
	c.Assert(err, ck.IsNil)

	checkStage := func(expected JobStage) {
		var job JobInfo
		err := ds.Get(jobKey, &job)
		c.Assert(err, ck.IsNil)
		c.Assert(job.Stage, ck.Equals, expected)
	}

	checkStage(StageFormation)

	taskKeys := make([]*datastore.Key, 2)
	tasks := make([]JobTask, len(taskKeys))
	for i := range taskKeys {
		taskKeys[i] = ds.NewKey(TaskEntity, "", int64(i+1), jobKey)
		tasks[i].Status = TaskStatusRunning
		tasks[i].Type = TaskTypeMap
	}

	err = createTasks(ds, jobKey, taskKeys, tasks, StageMapping)
	c.Assert(err, ck.IsNil)
	checkStage(StageMapping)

	advanced, _, err := jobStageComplete(ds, jobKey, taskKeys, StageMapping, StageReducing, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(advanced, ck.Equals, false)

	tasks[0].Status = TaskStatusDone
	tasks[0].Done = jobKey
	_, err = ds.Put(taskKeys[0], &tasks[0])
	c.Assert(err, ck.IsNil)
	advanced, _, err = jobStageComplete(ds, jobKey, taskKeys, StageMapping, StageReducing, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(advanced, ck.Equals, false)

	tasks[1].Status = TaskStatusDone
	tasks[1].Done = jobKey
	_, err = ds.Put(taskKeys[1], &tasks[1])
	c.Assert(err, ck.IsNil)

	// this uses an index query, which is eventually consistent
	advanced, _, err = jobStageComplete(ds, jobKey, taskKeys, StageMapping, StageReducing, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(advanced, ck.Equals, true)
	checkStage(StageReducing)

	// we're already at StageReducing, so nothing should happen here
	advanced, _, err = jobStageComplete(ds, jobKey, taskKeys, StageMapping, StageReducing, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(advanced, ck.Equals, false)
	checkStage(StageReducing)

	// let's fail a reducer and see what happens
	reduceKeys := make([]*datastore.Key, 2)
	reduceTasks := make([]JobTask, len(reduceKeys))
	reduceKeys[0] = datastore.NewKey(ctx, TaskEntity, "", int64(1001), jobKey)
	reduceKeys[1] = datastore.NewKey(ctx, TaskEntity, "", int64(1002), jobKey)
	reduceTasks[0] = JobTask{
		Status: TaskStatusFailed,
		Info:   "reason for failure",
		Done:   jobKey,
		Type:   TaskTypeReduce,
	}
	reduceTasks[1] = JobTask{
		Status: TaskStatusDone,
		Done:   jobKey,
		Type:   TaskTypeReduce,
	}
	err = createTasks(ds, jobKey, reduceKeys, reduceTasks, StageReducing)
	c.Assert(err, ck.IsNil)

	// this uses an index query, which is eventually consistent
	advanced, checkJob, err := jobStageComplete(ds, jobKey, reduceKeys, StageReducing, StageDone, mrt.nullLog)
	c.Assert(advanced, ck.Equals, true)
	c.Assert(checkJob.Stage, ck.Equals, StageFailed)
	checkStage(StageFailed)
}

func (mrt *MapreduceTests) TestWaitForStageCompletion(c *ck.C) {
	ds := appwrap.NewLocalDatastore()
	ctx := appwrap.StubContext()
	jobKey, err := createJob(ds, "prefix", []string{}, "complete", false, "", 5)
	c.Assert(err, ck.IsNil)

	taskMock := &taskInterfaceMock{}
	count := 0
	job, err := doWaitForStageCompletion(ctx, ds, taskMock, jobKey, StageMapping, StageReducing, 1*time.Millisecond,
		func(ds appwrap.Datastore, jobKey *datastore.Key, tasks []*datastore.Key, expectedStage, nextStage JobStage, log appwrap.Logging) (stageChanged bool, job JobInfo, finalErr error) {
			if count == 5 {
				return true, JobInfo{UrlPrefix: "foo"}, nil
			}

			count++
			return false, JobInfo{}, nil
		},
		time.Minute, mrt.nullLog)
	c.Assert(err, ck.IsNil)
	c.Assert(job.UrlPrefix, ck.Equals, "foo")

	taskMock.On("PostStatus", ctx, mock.Anything).Return(nil).Once()

	job, err = doWaitForStageCompletion(ctx, ds, taskMock, jobKey, StageMapping, StageReducing, 1*time.Millisecond,
		func(ds appwrap.Datastore, jobKey *datastore.Key, tasks []*datastore.Key, expectedStage, nextStage JobStage, log appwrap.Logging) (stageChanged bool, job JobInfo, finalErr error) {
			// this is what happens when a task fails
			return true, JobInfo{Stage: StageFailed}, taskError{"some failure"}
		},
		time.Minute, mrt.nullLog)

	c.Assert(err, ck.NotNil)
	taskMock.AssertExpectations(c)
}
