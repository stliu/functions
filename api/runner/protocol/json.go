package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

type JSONProtocol struct {
	in  io.Writer
	out io.Reader
}

func (p *JSONProtocol) Dispatch(stdin io.Reader, stdout io.Writer) error {
	var body bytes.Buffer
	io.Copy(&body, stdin)

	payload := struct {
		Payload string
	}{body.String()}
	if err := json.NewEncoder(p.in).Encode(payload); err != nil {
		return err
	}

	var tmp interface{}
	if err := json.NewDecoder(p.out).Decode(&tmp); err != nil {
	}

	return nil
}
