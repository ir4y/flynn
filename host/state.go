package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/boltdb/bolt"
	"github.com/flynn/flynn/host/types"
)

// TODO: prune old jobs?

type State struct {
	id string

	jobs map[string]*host.ActiveJob
	mtx  sync.RWMutex

	containers map[string]*host.ActiveJob              // container ID -> job
	listeners  map[string]map[chan host.Event]struct{} // job id -> listener list (ID "all" gets all events)
	listenMtx  sync.RWMutex
	attachers  map[string]map[chan struct{}]struct{}

	stateFileMtx  sync.Mutex
	stateFilePath string
	stateDb       *bolt.DB

	backend Backend
}

func NewState(id string) *State {
	return &State{
		id:         id,
		jobs:       make(map[string]*host.ActiveJob),
		containers: make(map[string]*host.ActiveJob),
		listeners:  make(map[string]map[chan host.Event]struct{}),
		attachers:  make(map[string]map[chan struct{}]struct{}),
	}
}

func (s *State) Restore(file string, backend Backend) error {
	s.stateFileMtx.Lock()
	defer s.stateFileMtx.Unlock()
	s.backend = backend
	s.stateFilePath = file
	s.initializePersistence()

	if err := s.stateDb.View(func(tx *bolt.Tx) error {
		jobsBucket := tx.Bucket([]byte("jobs"))
		backendBucket := tx.Bucket([]byte("backend"))

		// restore jobs
		if err := jobsBucket.ForEach(func(k, v []byte) error {
			job := &host.ActiveJob{}
			if err := json.Unmarshal(v, job); err != nil {
				return err
			}
			if job.ContainerID != "" {
				s.containers[job.ContainerID] = job
			}
			s.jobs[string(k)] = job
			return nil
		}); err != nil {
			return err
		}

		// hand opaque blob back to backend so it can do its restore
		backendBlob := backendBucket.Get([]byte("backend"))
		return backend.RestoreState(s.jobs, backendBlob)
	}); err != nil {
		return fmt.Errorf("could not restore from host persistence db: %s", err)
	}
	return nil
}

func (s *State) initializePersistence() {
	s.stateFileMtx.Lock()
	defer s.stateFileMtx.Unlock()

	// open/initialize db
	stateDb, err := bolt.Open(s.stateFilePath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		panic(fmt.Errorf("could not open db: %s", err))
	}
	s.stateDb = stateDb
	if err := s.stateDb.Update(func(tx *bolt.Tx) error {
		// idempotently create buckets.  (errors ignored because they're all compile-time impossible args checks.)
		tx.CreateBucketIfNotExists([]byte("jobs"))
		tx.CreateBucketIfNotExists([]byte("backend"))
		return nil
	}); err != nil {
		panic(fmt.Errorf("could not initialize host persistence db: %s", err))
	}
}

func (s *State) persist() {
	s.stateFileMtx.Lock()
	defer s.stateFileMtx.Unlock()
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	s.initializePersistence()

	if err := s.stateDb.Update(func(tx *bolt.Tx) error {
		jobsBucket := tx.Bucket([]byte("jobs"))
		backendBucket := tx.Bucket([]byte("backend"))

		// serialize each job, push into jobs bucket
		for jobName, job := range s.jobs {
			b, err := json.Marshal(job)
			err = jobsBucket.Put([]byte(jobName), b)
			if err != nil {
				return fmt.Errorf("could not persist job to boltdb: %s", err)
			}
		}

		// save the opaque blob the backend handed us as its state
		if b, ok := s.backend.(StateSaver); ok {
			if bytes, err := b.Serialize(); err != nil {
				return fmt.Errorf("error serializing backend state: %s", err)
			} else {
				err := backendBucket.Put([]byte("backend"), bytes)
				if err != nil {
					return fmt.Errorf("could not persist backend state to boltdb: %s", err)
				}
			}
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("could not persist to boltdb: %s", err))
	}
}

func (s *State) AddJob(j *host.Job, ip string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	job := &host.ActiveJob{Job: j, HostID: s.id, InternalIP: ip}
	s.jobs[j.ID] = job
	s.sendEvent(job, "create")
	go s.persist()
}

func (s *State) GetJob(id string) *host.ActiveJob {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	job := s.jobs[id]
	if job == nil {
		return nil
	}
	jobCopy := *job
	return &jobCopy
}

func (s *State) RemoveJob(id string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	delete(s.jobs, id)
	go s.persist()
}

func (s *State) Get() map[string]host.ActiveJob {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	res := make(map[string]host.ActiveJob, len(s.jobs))
	for k, v := range s.jobs {
		res[k] = *v
	}
	return res
}

func (s *State) ClusterJobs() []*host.Job {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	res := make([]*host.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		res = append(res, j.Job)
	}
	return res
}

func (s *State) SetContainerID(jobID, containerID string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.jobs[jobID].ContainerID = containerID
	s.containers[containerID] = s.jobs[jobID]
	go s.persist()
}

func (s *State) SetManifestID(jobID, manifestID string) {
	s.mtx.Lock()
	s.jobs[jobID].ManifestID = manifestID
	s.mtx.Unlock()
	go s.persist()
}

func (s *State) SetForceStop(jobID string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return
	}

	job.ForceStop = true
	go s.persist()
}

func (s *State) SetStatusRunning(jobID string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	job, ok := s.jobs[jobID]
	if !ok || job.Status != host.StatusStarting {
		return
	}

	job.StartedAt = time.Now().UTC()
	job.Status = host.StatusRunning
	s.sendEvent(job, "start")
	go s.persist()
}

func (s *State) SetContainerStatusDone(containerID string, exitCode int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	job, ok := s.containers[containerID]
	if !ok {
		return
	}
	s.setStatusDone(job, exitCode)
}

func (s *State) SetStatusDone(jobID string, exitCode int) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		fmt.Println("SKIP")
		return
	}
	s.setStatusDone(job, exitCode)
}

func (s *State) setStatusDone(job *host.ActiveJob, exitStatus int) {
	if job.Status == host.StatusDone || job.Status == host.StatusCrashed || job.Status == host.StatusFailed {
		return
	}
	job.EndedAt = time.Now().UTC()
	job.ExitStatus = exitStatus
	if exitStatus == 0 {
		job.Status = host.StatusDone
	} else {
		job.Status = host.StatusCrashed
	}
	s.sendEvent(job, "stop")
	go s.persist()
}

func (s *State) SetStatusFailed(jobID string, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	job, ok := s.jobs[jobID]
	if !ok || job.Status == host.StatusDone || job.Status == host.StatusCrashed || job.Status == host.StatusFailed {
		return
	}
	job.Status = host.StatusFailed
	job.EndedAt = time.Now().UTC()
	errStr := err.Error()
	job.Error = &errStr
	s.sendEvent(job, "error")
	go s.persist()
	go s.WaitAttach(jobID)
}

func (s *State) AddAttacher(jobID string, ch chan struct{}) *host.ActiveJob {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if job, ok := s.jobs[jobID]; ok {
		jobCopy := *job
		return &jobCopy
	}
	if _, ok := s.attachers[jobID]; !ok {
		s.attachers[jobID] = make(map[chan struct{}]struct{})
	}
	s.attachers[jobID][ch] = struct{}{}
	return nil
}

func (s *State) RemoveAttacher(jobID string, ch chan struct{}) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if a, ok := s.attachers[jobID]; ok {
		delete(a, ch)
		if len(a) == 0 {
			delete(s.attachers, jobID)
		}
	}
}

func (s *State) WaitAttach(jobID string) {
	s.mtx.Lock()
	a := s.attachers[jobID]
	delete(s.attachers, jobID)
	s.mtx.Unlock()
	for ch := range a {
		// signal attach
		ch <- struct{}{}
		// wait for attach
		<-ch
	}
}

func (s *State) AddListener(jobID string) chan host.Event {
	ch := make(chan host.Event)
	s.listenMtx.Lock()
	if _, ok := s.listeners[jobID]; !ok {
		s.listeners[jobID] = make(map[chan host.Event]struct{})
	}
	s.listeners[jobID][ch] = struct{}{}
	s.listenMtx.Unlock()
	return ch
}

func (s *State) RemoveListener(jobID string, ch chan host.Event) {
	go func() {
		// drain to prevent deadlock while removing the listener
		for range ch {
		}
	}()
	s.listenMtx.Lock()
	delete(s.listeners[jobID], ch)
	if len(s.listeners[jobID]) == 0 {
		delete(s.listeners, jobID)
	}
	s.listenMtx.Unlock()
	close(ch)
}

func (s *State) sendEvent(job *host.ActiveJob, event string) {
	j := *job
	go func() {
		s.listenMtx.RLock()
		defer s.listenMtx.RUnlock()
		e := host.Event{JobID: job.Job.ID, Job: &j, Event: event}
		for ch := range s.listeners["all"] {
			ch <- e
		}
		for ch := range s.listeners[job.Job.ID] {
			ch <- e
		}
	}()
}
