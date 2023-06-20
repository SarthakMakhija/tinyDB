package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestWalWithSingleEntry(t *testing.T) {
	wal, _ := NewWAL(1, ".")
	defer func() {
		_ = os.RemoveAll(wal.writableFileHandle.Name())
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

		require.Nil(t, err)
		require.Equal(t, key, string(entry.key))
		require.Equal(t, value, string(entry.value))
	}
}

func TestWalWithMultipleEntries(t *testing.T) {
	wal, _ := NewWAL(10, ".")
	defer func() {
		_ = os.RemoveAll(wal.writableFileHandle.Name())
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

		require.Nil(t, err)
		require.Equal(t, key, string(entry.key))
		require.Equal(t, expected[key], string(entry.value))
	}
}
