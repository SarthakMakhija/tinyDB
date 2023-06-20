package log

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWalWithSingleEntry(t *testing.T) {
	wal, _ := NewWAL(1, ".")
	defer func() {
		wal.Remove()
	}()
	defer func() {
		_ = wal.readableFileHandle.Close()
	}()

	_ = wal.Write(NewEntry([]byte("db"), []byte("tinyDB")))
	_ = wal.writableFileHandle.Close()

	readOnlyWal, _ := NewReadonlyWAL(1, ".")
	iterator := readOnlyWal.Iterator()

	expected := make(map[string]string)
	expected["db"] = "tinyDB"

	for key, value := range expected {
		entry, err := iterator.Next()

		assert.Nil(t, err)
		assert.Equal(t, key, string(entry.key))
		assert.Equal(t, value, string(entry.value))
	}
}

func TestWalWithMultipleEntries(t *testing.T) {
	wal, _ := NewWAL(10, ".")
	defer func() {
		wal.Remove()
	}()
	defer func() {
		_ = wal.readableFileHandle.Close()
	}()

	_ = wal.Write(NewEntry([]byte("db"), []byte("tinyDB")))
	_ = wal.Write(NewEntry([]byte("type"), []byte("relational")))
	_ = wal.Write(NewEntry([]byte("storage"), []byte("LSM")))

	_ = wal.writableFileHandle.Close()

	readOnlyWal, _ := NewReadonlyWAL(10, ".")
	iterator := readOnlyWal.Iterator()

	expected := make(map[string]string)
	expected["db"] = "tinyDB"
	expected["type"] = "relational"
	expected["storage"] = "LSM"

	keys := []string{"db", "type", "storage"}
	for _, key := range keys {
		entry, err := iterator.Next()

		assert.Nil(t, err)
		assert.Equal(t, key, string(entry.key))
		assert.Equal(t, expected[key], string(entry.value))
	}
}
