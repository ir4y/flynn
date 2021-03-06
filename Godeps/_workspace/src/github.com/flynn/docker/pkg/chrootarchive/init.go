package chrootarchive

import (
	"fmt"
	"os"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/docker/pkg/reexec"
)

func init() {
	reexec.Register("docker-untar", untar)
	reexec.Register("docker-applyLayer", applyLayer)
}

func fatal(err error) {
	fmt.Fprint(os.Stderr, err)
	os.Exit(1)
}
