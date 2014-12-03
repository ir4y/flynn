package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
)

type Worker struct {
	q       *Queue
	name    string
	handler Handler
	mtx     sync.Mutex
	stopped bool
}

type Job struct {
	ID   string
	Data []byte
	Err  error
}

func newWorker(q *Queue, name string, h Handler) *Worker {
	return &Worker{
		q:       q,
		name:    name,
		handler: h,
	}
}

func (w *Worker) Start() error {
	for {
		if w.isStopped() {
			return nil
		}
		if err := w.work(); err != nil {
			return err
		}
	}
}

func (w *Worker) Stop() {
	w.mtx.Lock()
	w.stopped = true
	w.mtx.Unlock()
}

func (w *Worker) isStopped() bool {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	return w.stopped
}

func (w *Worker) work() error {
	job, err := w.lockJob()
	if err != nil {
		return err
	}
	w.q.Notify(job, JobStateRunning)
	if err := w.handler(job); err != nil {
		job.Err = err
		w.q.Notify(job, JobStateFailed)
		w.unlock(job)
		return nil
	}
	w.q.Notify(job, JobStateDone)
	w.remove(job)
	return nil
}

var errIsStopped = errors.New("queue: is stopped")

func (w *Worker) lockJob() (*Job, error) {
	job := &Job{}
	for {
		if w.isStopped() {
			return nil, errIsStopped
		}
		err := w.q.DB.QueryRow("SELECT id, data FROM lock_head($1)", w.name).Scan(&job.ID, &job.Data)
		if err == sql.ErrNoRows {
			if err := w.q.Wait(w.name); err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		return job, nil
	}
}

func (w *Worker) unlock(job *Job) {
	w.q.DB.Exec(fmt.Sprintf("UPDATE %s set locked_at = null where id = $1", w.q.Table), job.ID)
}

func (w *Worker) remove(job *Job) {
	w.q.DB.Exec(fmt.Sprintf("DELETE FROM %s where id = $1", w.q.Table), job.ID)
}
