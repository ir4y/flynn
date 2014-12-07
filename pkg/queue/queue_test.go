package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/pkg/testutils"
)

// Hook gocheck up to the "go test" runner
func Test(t *testing.T) { TestingT(t) }

type S struct {
	q *Queue
}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
	dbname := "queuetest"
	c.Assert(testutils.SetupPostgres(dbname), IsNil)

	dsn := fmt.Sprintf("dbname=%s", dbname)
	db, err := sql.Open("postgres", dsn)
	c.Assert(err, IsNil)

	s.q = New(db, "jobs")
	c.Assert(s.q.SetupDB(), IsNil)
}

func (s *S) stopWorker(c *C, w *Worker) {
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.Fatal("timed out stopping worker")
	}
}

func waitForEventCondition(c *C, ch chan Event, condition func(Event) bool) {
	for {
		select {
		case e := <-ch:
			if condition(e) {
				return
			}
		case <-time.After(time.Second):
			c.Fatal("timed out waiting for job event")
		}
	}
}

func (s *S) TestWork(c *C) {
	name := "test-work"
	payload := "a message"

	var job *Job
	s.q.Handle(name, func(j *Job) error {
		job = j
		return nil
	})

	ch := s.q.Subscribe(name)
	defer s.q.Unsubscribe(name, ch)

	data, err := json.Marshal(payload)
	c.Assert(err, IsNil)
	c.Assert(s.q.Enqueue(name, data), IsNil)

	w, err := s.q.Worker(name, 1)
	c.Assert(err, IsNil)
	defer s.stopWorker(c, w)
	go w.Start()

	waitForEventCondition(c, ch, func(e Event) bool {
		return e.State == JobStateDone
	})
	var actual string
	c.Assert(json.Unmarshal(job.Data, &actual), IsNil)
	c.Assert(actual, Equals, payload)
}

func (s *S) TestWorkNoHandler(c *C) {
	_, err := s.q.Worker("nonexistent", 1)
	c.Assert(err, Equals, ErrNoHandler)
}

func (s *S) TestWorkMultipleJobs(c *C) {
	name := "test-multiple-jobs"

	var mtx sync.Mutex
	count := make(map[string]int)
	s.q.Handle(name, func(j *Job) error {
		mtx.Lock()
		count[j.ID]++
		mtx.Unlock()
		return nil
	})

	ch := s.q.Subscribe(name)
	defer s.q.Unsubscribe(name, ch)

	expected := 10
	for i := 0; i < expected; i++ {
		c.Assert(s.q.Enqueue(name, []byte("job")), IsNil)
	}

	w, err := s.q.Worker(name, 5)
	c.Assert(err, IsNil)
	defer s.stopWorker(c, w)
	go w.Start()

	actual := 0
	waitForEventCondition(c, ch, func(e Event) bool {
		if e.State == JobStateDone {
			actual++
		}
		return actual >= expected
	})
	c.Assert(len(count), Equals, expected)
}
