package mlc

import (
	"bytes"
	"encoding/gob"
)

type EncoderDecoder interface {
	Encode(*NodeMeta) []byte
	Decode([]byte) NodeMeta
}

type DefaultEncoderDecoder struct{}

func (d *DefaultEncoderDecoder) Encode(m *NodeMeta) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(m)
	return buf.Bytes()
}

func (d *DefaultEncoderDecoder) Decode(data []byte) NodeMeta {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var m NodeMeta
	dec.Decode(&m)
	return m
}
