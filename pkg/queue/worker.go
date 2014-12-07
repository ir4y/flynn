package queue

import (
	"database/sql"
	"fmt"
	"sync"
)

type Worker struct {
	q           *Queue
	name        string
	handler     Handler
	mtx         sync.Mutex
	stop        chan struct{}
	done        chan struct{}
	concurrency int
}

type Job struct {
	ID   string
	Data []byte
	Err  error
}

func newWorker(q *Queue, name string, h Handler, concurrency int) *Worker {
	return &Worker{
		q:           q,
		name:        name,
		handler:     h,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		concurrency: concurrency,
	}
}

func (w *Worker) Start() error {
	defer close(w.done)

	// pool controls how many jobs are worked concurrently
	pool := make(chan struct{}, w.concurrency)
	for i := 0; i < w.concurrency; i++ {
		pool <- struct{}{}
	}
	wait := func() {
		for i := 0; i < w.concurrency; i++ {
			<-pool
		}
	}

	for {
		select {
		case <-w.stop:
			wait()
			return nil
		case <-pool:
			job, err := w.lockJob()
			if err != nil {
				pool <- struct{}{}
				wait()
				return err
			}
			go func() {
				w.work(job)
				pool <- struct{}{}
			}()
		}
	}
}

func (w *Worker) Stop() {
	close(w.stop)
	<-w.done
}

func (w *Worker) work(job *Job) {
	w.q.Notify(job, JobStateRunning)
	if err := w.handler(job); err != nil {
		job.Err = err
		w.q.Notify(job, JobStateFailed)
		w.unlock(job)
		return
	}
	w.q.Notify(job, JobStateDone)
	w.remove(job)
}

func (w *Worker) lockJob() (*Job, error) {
	job := &Job{}
	for {
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
