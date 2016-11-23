package protocol

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
)

// HTTPProtocol converts stdin/stdout streams into HTTP/1.1 compliant
// communication. It relies on Content-Length to know when to stop reading from
// containers stdout. It also mandates valid HTTP headers back and forth, thus
// returning errors in case of parsing problems.
type HTTPProtocol struct {
	in  io.Writer
	out io.Reader
}

func (p *HTTPProtocol) IsStreamable() bool {
	return true
}

func (p *HTTPProtocol) Dispatch(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var retErr error
	done := make(chan struct{})
	go func() {
		var body bytes.Buffer
		io.Copy(&body, stdin)
		req, err := http.NewRequest("GET", "/", &body)
		if err != nil {
			retErr = err
			return
		}
		req.Header.Set("Content-Length", fmt.Sprint(body.Len()))
		raw, err := httputil.DumpRequest(req, true)
		if err != nil {
			retErr = err
			return
		}
		p.in.Write(raw)

		res, err := http.ReadResponse(bufio.NewReader(p.out), req)
		if err != nil {
			retErr = err
			return
		}

		io.Copy(stdout, res.Body)
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-done:
		return retErr
	}
}
