package main

import (
	"io"

	"github.com/flynn/flynn/host/types"
)

type AttachRequest struct {
	Job    *host.ActiveJob
	Logs   bool
	Stream bool
	Height uint16
	Width  uint16

	Attached chan struct{}

	Stdout io.WriteCloser
	Stderr io.WriteCloser
	Stdin  io.Reader
}

type Backend interface {
	Run(*host.Job) error
	Stop(string) error
	Signal(string, int) error
	ResizeTTY(id string, height, width uint16) error
	Attach(*AttachRequest) error
	Cleanup() error
	UnmarshalState(map[string]*host.ActiveJob, map[string][]byte, []byte) error
}

type StateSaver interface {
	MarshalJobState(jobID string) ([]byte, error)
	MarshalGlobalState() ([]byte, error)
}
