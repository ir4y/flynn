package queue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/pq"
)

type Handler func(*Job) error

type Queue struct {
	DB    *sql.DB
	Table string

	mtx          sync.Mutex
	handlers     map[string]Handler
	listener     *pq.Listener
	listenErr    chan error
	subscribers  map[string]map[chan Event]struct{}
	subscribeMtx sync.RWMutex
}

func New(db *sql.DB, table string) *Queue {
	q := &Queue{
		DB:          db,
		Table:       table,
		handlers:    make(map[string]Handler),
		listenErr:   make(chan error),
		subscribers: make(map[string]map[chan Event]struct{}),
	}
	q.listener = pq.NewListener(db.DSN(), 10*time.Second, time.Minute, q.listenEvent)
	return q
}

func (q *Queue) listenEvent(ev pq.ListenerEventType, err error) {
	if ev == pq.ListenerEventConnectionAttemptFailed {
		q.listenErr <- err
	}
}

func (q *Queue) Enqueue(name string, data []byte) (err error) {
	_, err = q.DB.Exec(fmt.Sprintf("INSERT INTO %s (q_name, data) VALUES ($1, $2)", q.Table), name, data)
	return
}

func (q *Queue) Handle(name string, h Handler) {
	q.mtx.Lock()
	q.handlers[name] = h
	q.mtx.Unlock()
}

var ErrNoHandler = errors.New("queue: no handler registered")

func (q *Queue) Worker(name string, concurrency int) (*Worker, error) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	h, ok := q.handlers[name]
	if !ok {
		return nil, ErrNoHandler
	}
	return newWorker(q, name, h, concurrency), nil
}

const waitTimeout = 5 * time.Second

var errWaitTimeout = errors.New("queue: wait timeout")

func (q *Queue) Wait(name string) error {
	if err := q.listener.Listen(name); err != nil && err != pq.ErrChannelAlreadyOpen {
		return err
	}
	select {
	case err := <-q.listenErr:
		return err
	case <-time.After(waitTimeout):
		return errWaitTimeout
	case <-q.listener.Notify:
		return nil
	}
}

type Event struct {
	Job   *Job
	State JobState
}

type JobState uint8

const (
	JobStateRunning JobState = iota
	JobStateDone
	JobStateFailed
)

func (q *Queue) Notify(job *Job, state JobState) {
	go func() {
		q.subscribeMtx.RLock()
		defer q.subscribeMtx.RUnlock()
		e := Event{Job: job, State: state}
		for _, s := range q.subscribers {
			for ch := range s {
				ch <- e
			}
		}
	}()
}

func (q *Queue) Subscribe(name string) chan Event {
	ch := make(chan Event)
	q.subscribeMtx.Lock()
	if _, ok := q.subscribers[name]; !ok {
		q.subscribers[name] = make(map[chan Event]struct{})
	}
	q.subscribers[name][ch] = struct{}{}
	q.subscribeMtx.Unlock()
	return ch
}

func (q *Queue) Unsubscribe(name string, ch chan Event) {
	go func() {
		// drain to prevent deadlock while removing the listener
		for range ch {
		}
	}()
	q.subscribeMtx.Lock()
	delete(q.subscribers[name], ch)
	if len(q.subscribers[name]) == 0 {
		delete(q.subscribers, name)
	}
	q.subscribeMtx.Unlock()
	close(ch)
}
