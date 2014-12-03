package queue

import (
	"encoding/json"
	"fmt"
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

func (s *S) TestWork(c *C) {
	name := "test-start"
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

	worker, err := s.q.Work(name)
	c.Assert(err, IsNil)
	defer worker.Stop()
	go worker.Start()

loop:
	for {
		select {
		case e := <-ch:
			if e.State == JobStateDone {
				break loop
			}
		case <-time.After(time.Second):
			c.Fatal("timed out waiting for queue event")
		}
	}
	var actual string
	c.Assert(json.Unmarshal(job.Data, &actual), IsNil)
	c.Assert(actual, Equals, payload)
}

func (s *S) TestWorkNoHandler(c *C) {
	_, err := s.q.Work("nonexistent")
	c.Assert(err, Equals, ErrNoHandler)
}
