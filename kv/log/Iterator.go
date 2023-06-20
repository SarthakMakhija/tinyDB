package log

import (
	"bufio"
	"os"
)

type BufferedReader struct {
	*bufio.Reader
}

type WalIterator struct {
	reader *BufferedReader
}

func NewBufferedReader(file *os.File) *BufferedReader {
	return &BufferedReader{
		bufio.NewReader(file),
	}
}

func (iterator *WalIterator) Next() (*Entry, error) {
	header := new(Header)
	err := header.decodeFrom(iterator.reader)
	if err != nil {
		return nil, err
	}
	entry := new(Entry)
	err = entry.decodeFrom(header, iterator.reader)
	if err != nil {
		return nil, err
	}
	return entry, nil
}
