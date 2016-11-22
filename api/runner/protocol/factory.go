package protocol

import (
	"errors"
	"io"
)

var errInvalidProtocol = errors.New("Invalid Protocol")

type ContainerIO interface {
	Dispatch(stdin io.Reader, stdout io.Writer) error
}

type Protocol int

const (
	HTTP Protocol = iota
	JSON
)

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
