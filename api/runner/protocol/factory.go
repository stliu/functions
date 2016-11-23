package protocol

import (
	"errors"
	"io"

	"github.com/iron-io/functions/api/models"
)

var errInvalidProtocol = errors.New("Invalid Protocol")

// ContainerIO defines the interface used to talk to a hot container.
// Internally, a protocol must know when to alternate between stdin and stdout.
// It returns any protocol error, if present.
type ContainerIO interface {
	Dispatch(stdin io.Reader, stdout io.Writer) error
}

// Protocol defines all non-default protocols that operates a ContainerIO. If
// a protocol is being used, then it must be a hot container.
type Protocol string

// Hot container protocols
const (
	HTTP Protocol = models.FormatHTTP
	JSON Protocol = models.FormatJSON
)

// New creates a valid protocol handler from a I/O pipe representing containers
// stdin/stdout.
func New(p Protocol, in io.Writer, out io.Reader) (ContainerIO, error) {
	switch p {
	case HTTP:
		return &HTTPProtocol{in, out}, nil
	case JSON:
		return &JSONProtocol{in, out}, nil
	default:
		return nil, errInvalidProtocol
	}
}
