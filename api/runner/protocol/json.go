package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
)

// JSONProtocol converts stdin/stdout streams into JSON calls. It assumes that
// every incoming request has at least one valid JSON completely parsable. As
// soon as the first incoming JSON is properly parsed, it alternates to
// containers stdout, which will parse until the first outcoming JSON is finally
// parsed.
type JSONProtocol struct {
	in  io.Writer
	out io.Reader
}

func (p *JSONProtocol) IsStreamable() bool {
	return true
}

func (p *JSONProtocol) Dispatch(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var retErr error
	done := make(chan struct{})
	go func() {
		var body bytes.Buffer
		io.Copy(&body, stdin)

		payload := struct {
			Payload string
		}{body.String()}
		if err := json.NewEncoder(p.in).Encode(payload); err != nil {
			retErr = err
			return
		}

		var tmp interface{}
		json.NewDecoder(p.out).Decode(&tmp)
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
