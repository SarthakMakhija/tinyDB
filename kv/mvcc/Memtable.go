package mvcc

import (
	"sync"
	"tinydb/kv"
	"tinydb/kv/log"
	"tinydb/kv/mvcc/utils"
)

const MaxHeight = 20

// MemTable is an in-memory structure built on top of SkipList.
type MemTable struct {
	lock           sync.RWMutex
	head           *SkiplistNode
	levelGenerator utils.LevelGenerator
	wal            *log.WAL
}

// NewMemTable creates a new instance of MemTable.
func NewMemTable(fileId uint64, options *kv.Options) (*MemTable, error) {
	wal, err := log.NewWAL(fileId, options.DbDirectory)
	if err != nil {
		return nil, err
	}
	return &MemTable{
		head:           newSkiplistNode(emptyVersionedKey(), emptyValue(), MaxHeight),
		levelGenerator: utils.NewLevelGenerator(MaxHeight),
		wal:            wal,
	}, nil
}

// PutOrUpdate puts or updates the key and the value pair in the SkipList.
func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) error {
	err := memTable.wal.Write(log.NewEntry(key.encode(), value.Slice()))
	if err != nil {
		return err
	}
	memTable.lock.Lock()
	defer memTable.lock.Unlock()

	memTable.head.putOrUpdate(key, value, memTable.levelGenerator)
	return nil
}

// Get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	memTable.lock.RLock()
	defer memTable.lock.RUnlock()

	return memTable.head.get(key)
}

// RemoveWAL removes the WAL file.
func (memTable *MemTable) RemoveWAL() {
	memTable.wal.Remove()
}
