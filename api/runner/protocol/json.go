package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/iron-io/functions/api/models"
	"github.com/iron-io/functions/api/runner/task"
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

func (p *JSONProtocol) Dispatch(ctx context.Context, t task.Request) error {
	var retErr error
	done := make(chan struct{})
	go func() {
		var body bytes.Buffer
		io.Copy(&body, t.Config.Stdin)

		payload := struct {
			Env     map[string]string
			Payload string
		}{t.Config.Env, body.String()}
		if err := json.NewEncoder(p.in).Encode(payload); err != nil {
			retErr = err
			return
		}

		var tmp interface{}
		json.NewDecoder(p.out).Decode(&tmp)
		json.NewEncoder(t.Config.Stdout).Encode(tmp)
		done <- struct{}{}
	}()

	timeout := time.After(t.Config.Timeout)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timeout:
		return models.ErrRunnerTimeout
	case <-done:
		return nil
	}
}
