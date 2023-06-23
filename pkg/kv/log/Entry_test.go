package log

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestHeaderEncodeAndDecode(t *testing.T) {
	header := new(Header)
	header.keyLength = 5
	header.valueLength = 15

	encodedHeader := header.encode()

	decodedHeader := new(Header)
	_ = decodedHeader.decodeFrom(strings.NewReader(string(encodedHeader)))
	assert.Equal(t, uint32(5), decodedHeader.keyLength)
	assert.Equal(t, uint32(15), decodedHeader.valueLength)
}

func TestHeaderEncodeAndDecodeWithError(t *testing.T) {
	header := new(Header)
	header.keyLength = 5
	header.valueLength = 15

	encodedHeader := header.encode()

	decodedHeader := new(Header)
	err := decodedHeader.decodeFrom(strings.NewReader(string(encodedHeader[0:2])))
	assert.Error(t, err)
}

func TestEntryEncodeAndDecode(t *testing.T) {
	entry := NewEntry([]byte("storage"), []byte("LSM"))
	encodedEntry, _ := entry.Encode()

	reader := strings.NewReader(string(encodedEntry))

	decodedEntry := new(Entry)
	decodedHeader := new(Header)
	_ = decodedHeader.decodeFrom(reader)
	_ = decodedEntry.decodeFrom(decodedHeader, reader)

	assert.Equal(t, "storage", string(decodedEntry.key))
	assert.Equal(t, "LSM", string(decodedEntry.value))
}

func TestEntryEncodeAndDecodeWithError(t *testing.T) {
	entry := NewEntry([]byte("storage"), []byte("LSM"))
	encodedEntry, _ := entry.Encode()

	reader := strings.NewReader(string(encodedEntry[0:3]))

	decodedEntry := new(Entry)
	decodedHeader := new(Header)
	_ = decodedHeader.decodeFrom(reader)
	err := decodedEntry.decodeFrom(decodedHeader, reader)

	assert.Error(t, err)
}
