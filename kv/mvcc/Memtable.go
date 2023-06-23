package mvcc

import (
	"tinydb/kv"
	"tinydb/kv/log"
)

// MemTable is an in-memory structure built on top of SkipList.
type MemTable struct {
	skiplist *Skiplist
	wal      *log.WAL
}

// NewMemTable creates a new instance of MemTable.
func NewMemTable(fileId uint64, options *kv.Options) (*MemTable, error) {
	wal, err := log.NewWAL(fileId, options.DbDirectory)
	if err != nil {
		return nil, err
	}
	return &MemTable{
		skiplist: newSkiplist(),
		wal:      wal,
	}, nil
}

// PutOrUpdate puts or updates the key and the value pair in the SkipList and the associated WA:.
func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) error {
	err := memTable.wal.Write(log.NewEntry(key.encode(), value.ValueSlice()))
	if err != nil {
		return err
	}

	memTable.skiplist.putOrUpdate(key, value)
	return nil
}

// Delete deletes the key.
// Deletion is not a physical deletion.
// Deletion involves: Creating a new Entry with a NewDeletedValue and appending the entry in the WAL.
// The Key and the NewDeletedValue are added to the Skiplist.
func (memTable *MemTable) Delete(key VersionedKey) error {
	deletedValue := NewDeletedValue()
	err := memTable.wal.Write(log.NewEntry(key.encode(), deletedValue.encode()))
	if err != nil {
		return err
	}
	memTable.skiplist.putOrUpdate(key, deletedValue)
	return nil
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
