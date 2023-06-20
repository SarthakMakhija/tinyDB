package log

import (
	"fmt"
	"os"
)

type WAL struct {
	writableFileHandle *os.File
	readableFileHandle *os.File
}

func NewWAL(fileId uint64, directory string) (*WAL, error) {
	filePath := directory + fmt.Sprintf("%v.wal", fileId)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{writableFileHandle: file}, nil
}

func NewReadonlyWAL(fileId uint64, directory string) (*WAL, error) {
	filePath := directory + fmt.Sprintf("%v.wal", fileId)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}
	return &WAL{readableFileHandle: file}, nil
}

func (wal *WAL) Write(entry *Entry) error {
	encodedEntry, err := entry.Encode()
	if err != nil {
		return err
	}
	bytesWritten, err := wal.writableFileHandle.Write(encodedEntry)
	if err != nil {
		return err
	}
	if bytesWritten < len(encodedEntry) {
		return fmt.Errorf("could not append %v bytes to the WAL", len(encodedEntry))
	}
	return nil
}

func (wal *WAL) Remove() {
	err := os.RemoveAll(wal.writableFileHandle.Name())
	if err != nil {
		//TODO: Removes println in favor of logging
		println("error while closing WAL file ", wal.writableFileHandle.Name())
	}
}

func (wal *WAL) Iterator() *WalIterator {
	return &WalIterator{reader: NewBufferedReader(wal.readableFileHandle)}
}
