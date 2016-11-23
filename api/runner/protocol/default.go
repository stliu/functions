package protocol

import (
	"context"
	"io"
)

// DefaultProtocol is the protocol used by cold-containers
type DefaultProtocol struct {
}

func (p *DefaultProtocol) IsStreamable() bool {
	return false
}

func (p *DefaultProtocol) Dispatch(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	return nil
}
