package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
)

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
