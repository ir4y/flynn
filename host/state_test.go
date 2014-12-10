package main

import (
	"path/filepath"
	"testing"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/host/types"
)

func Test(t *testing.T) { TestingT(t) }

type S struct{}

var _ = Suite(&S{})

func (S) TestStateHostID(c *C) {
	workdir := c.MkDir()
	hostID := "abc123"
	state := NewState(hostID, filepath.Join(workdir, "host-state-db"))
	defer state.persistenceDbClose()
	state.AddJob(&host.Job{ID: "a"}, "1.1.1.1")
	job := state.GetJob("a")
	if job.HostID != hostID {
		c.Errorf("expected job.HostID to equal %s, got %s", hostID, job.HostID)
	}
}

type MockBackend struct{}

func (MockBackend) Run(*host.Job) error                                   { return nil }
func (MockBackend) Stop(string) error                                     { return nil }
func (MockBackend) Signal(string, int) error                              { return nil }
func (MockBackend) ResizeTTY(id string, height, width uint16) error       { return nil }
func (MockBackend) Attach(*AttachRequest) error                           { return nil }
func (MockBackend) Cleanup() error                                        { return nil }
func (MockBackend) RestoreState(map[string]*host.ActiveJob, []byte) error { return nil }

func (S) TestStatePersistRestore(c *C) {
	workdir := c.MkDir()
	hostID := "abc123"
	state := NewState(hostID, filepath.Join(workdir, "host-state-db"))
	state.stateSaveListener = make(chan struct{}, 1)
	state.AddJob(&host.Job{ID: "a"}, "1.1.1.1")
	<-state.stateSaveListener
	state.persistenceDbClose()

	// exercise the restore path.  failures will panic.
	// note that this does not test backend deserialization (the mock, obviously, isn't doing anything).
	state = NewState(hostID, filepath.Join(workdir, "host-state-db"))
	defer state.persistenceDbClose()
	state.Restore(&MockBackend{})

	// check we actually got job data back
	job := state.GetJob("a")
	if job.HostID != hostID {
		c.Errorf("expected job.HostID to equal %s, got %s", hostID, job.HostID)
	}
}
