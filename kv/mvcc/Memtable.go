package mvcc

import (
	"tinydb/kv"
	"tinydb/kv/log"
)

// MemTable is an in-memory structure built on top of SkipList.
type MemTable struct {
	skiplist *Skiplist
	wal      *log.WAL
	options  *kv.Options
}

// NewMemTable creates a new instance of MemTable.
// TODO: Validate options
func NewMemTable(fileId uint64, options *kv.Options) (*MemTable, error) {
	wal, err := log.NewWAL(fileId, options.DbDirectory)
	if err != nil {
		return nil, err
	}
	return &MemTable{
		skiplist: newSkiplist(),
		wal:      wal,
		options:  options,
	}, nil
}

// PutOrUpdate puts or updates the key and the value pair in the associated WAL and the SkipList.
func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) error {
	return memTable.write(key, value)
}

// Delete deletes the key.
// Deletion is not a physical deletion.
// Deletion involves: Creating a new Entry with a NewDeletedValue and appending the entry in the WAL.
// The Key and the NewDeletedValue are added to the Skiplist.
func (memTable *MemTable) Delete(key VersionedKey) error {
	return memTable.write(key, NewDeletedValue())
}

// Get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	return memTable.skiplist.get(key)
}

// RemoveWAL removes the WAL file.
func (memTable *MemTable) RemoveWAL() {
	memTable.wal.Remove()
}

// write to WAL and Skiplist.
func (memTable *MemTable) write(key VersionedKey, value Value) error {
	err := memTable.wal.Write(log.NewEntry(key.encode(), value.encode()))
	if err != nil {
		return err
	}
	memTable.skiplist.putOrUpdate(key, value)
	return nil
}

func (memTable *MemTable) isFull() bool {
	if memTable.skiplist.size >= memTable.options.MemtableSizeInBytes {
		return true
	}
	//TODO: Check WAL Size
	return false
}
