package protocol

import (
	"bufio"
	"bytes"
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

func (p *HTTPProtocol) Dispatch(stdin io.Reader, stdout io.Writer) error {
	var body bytes.Buffer
	io.Copy(&body, stdin)
	req, err := http.NewRequest("GET", "/", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", fmt.Sprint(body.Len()))
	raw, err := httputil.DumpRequest(req, true)
	if err != nil {
		return err
	}
	p.in.Write(raw)

	res, err := http.ReadResponse(bufio.NewReader(p.out), req)
	if err != nil {
		return err
	}

	io.Copy(stdout, res.Body)
	return nil
}
