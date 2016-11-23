package protocol

import (
	"bytes"
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
