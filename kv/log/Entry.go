package log

import (
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"
)

const (
	KeyLength    = unsafe.Sizeof(uint32(0))
	ValueLength  = unsafe.Sizeof(uint32(0))
	HeaderLength = KeyLength + ValueLength
)

type Header struct {
	keyLength   uint32
	valueLength uint32
}

func (header *Header) encode() []byte {
	encodedHeader := make([]byte, HeaderLength)
	binary.LittleEndian.PutUint32(encodedHeader[:], header.keyLength)
	binary.LittleEndian.PutUint32(encodedHeader[KeyLength:], header.valueLength)
	return encodedHeader
}

func (header *Header) decodeFrom(reader io.Reader) error {
	keyLengthBytes, valueLengthBytes := make([]byte, KeyLength), make([]byte, ValueLength)
	if _, err := reader.Read(keyLengthBytes); err != nil {
		return err
	}
	if _, err := reader.Read(valueLengthBytes); err != nil {
		return err
	}
	header.keyLength = binary.LittleEndian.Uint32(keyLengthBytes)
	header.valueLength = binary.LittleEndian.Uint32(valueLengthBytes)

	return nil
}

type Entry struct {
	key   []byte
	value []byte
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		key:   key,
		value: value,
	}
}

func (entry *Entry) Encode() ([]byte, error) {
	header := &Header{
		keyLength:   uint32(len(entry.key)),
		valueLength: uint32(len(entry.value)),
	}
	//TODO: use pool
	encoded := &bytes.Buffer{}
	if _, err := entry.writeTo(header.encode(), encoded); err != nil {
		return nil, err
	}
	if _, err := entry.writeTo(entry.key, encoded); err != nil {
		return nil, err
	}
	if _, err := entry.writeTo(entry.value, encoded); err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

func (entry *Entry) writeTo(part []byte, buffer *bytes.Buffer) (int, error) {
	return buffer.Write(part)
}

func (entry *Entry) decodeFrom(header *Header, reader io.Reader) error {
	keyBytes, valueBytes := make([]byte, header.keyLength), make([]byte, header.valueLength)
	if _, err := reader.Read(keyBytes); err != nil {
		return err
	}
	if _, err := reader.Read(valueBytes); err != nil {
		return err
	}
	entry.key = keyBytes
	entry.value = valueBytes

	return nil
}
