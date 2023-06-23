package kv

import (
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
)

// Workspace
// TODO: Check if we need any locks (during get), because put/delete will happen serially on the commit of a transaction;
// TODO: but get can run concurrently.
// Workspace is an abstraction that deals with active and all the immutable memtables.
// This abstraction will be instantiated once in the lifetime of the entire appplication.
type Workspace struct {
	activeMemTable     *mvcc.MemTable
	immutableMemTables []*mvcc.MemTable
	options            *option.Options
}

// NewWorkspace creates a new instance of Workspace.
// Returns an error if the creation of NewMemtable fails.
func NewWorkspace(options *option.Options) (*Workspace, error) {
	memtable, err := mvcc.NewMemTable(0, options)
	if err != nil {
		return nil, err
	}
	return &Workspace{
		activeMemTable: memtable,
		options:        options,
	}, nil
}

// PutOrUpdate puts or updates the key and the value pair in the active memtable.
// It ensures that the memtable has the space to accommodate the incoming Key/Value pair.
// Refer to IsFull() method inside tinydb/pkg/kv/mvcc.MemTable.
func (workspace *Workspace) PutOrUpdate(key mvcc.VersionedKey, value mvcc.Value) error {
	if err := workspace.ensureRoom(); err != nil {
		return err
	}
	return workspace.activeMemTable.PutOrUpdate(key, value)
}

// Delete deletes the key from the active memtable.
// Deletion is not a physical deletion, it is another put with a version number.
// It ensures that the memtable has the space to accommodate the incoming Key/Value pair.
// Refer to IsFull() method inside tinydb/pkg/kv/mvcc.MemTable.
func (workspace *Workspace) Delete(key mvcc.VersionedKey) error {
	if err := workspace.ensureRoom(); err != nil {
		return err
	}
	return workspace.activeMemTable.Delete(key)
}

// Get returns a pair of (ValueWithVersion, bool) for the incoming key.
// It returns (ValueWithVersion, true) if the value exists for the incoming key, else (nil, false).
// It searches the active memtable and all the immutable memtables from the last index to 0 and tries
// to find the key with the closest version.
func (workspace *Workspace) Get(key mvcc.VersionedKey) (mvcc.ValueWithVersion, bool) {
	valueWithMaxVersion := mvcc.EmptyValueWithZeroVersion()
	for _, memtable := range workspace.allMemtables() {
		value, ok := memtable.Get(key)
		if ok {
			if value.Version == key.Version {
				return value, ok
			}
			if value.Version > valueWithMaxVersion.Version {
				valueWithMaxVersion = value
			}
		}
	}
	//TODO: Read from SSTables arranged in levels
	if valueWithMaxVersion.Version > 0 {
		return valueWithMaxVersion, true
	}
	return valueWithMaxVersion, false
}

// ensureRoom ensures that the active memtable has the room to accommodate the incoming key/value pair.
// If the active memtable is full, a new memtable is created and the previously active memtable is added to the list of immutable memtables.
// TODO: Send the active memtable to be written to disk
func (workspace *Workspace) ensureRoom() error {
	if !workspace.activeMemTable.IsFull() {
		return nil
	}
	memtable, err := mvcc.NewMemTable(1, workspace.options)
	if err != nil {
		return err
	}
	workspace.immutableMemTables = append(workspace.immutableMemTables, workspace.activeMemTable)
	workspace.activeMemTable = memtable
	return nil
}

// allMemtables returns a slice of all the memtables includes: the currently active memtable and all the immutable memtables.
// the currently active memtable is placed in the index 0 of the allMemtables slice
// all the other immutable memtables are placed in the order of the latest immutable memtable first to
// the oldest immutable memtable last in the allMemtables slice.
func (workspace *Workspace) allMemtables() []*mvcc.MemTable {
	allMemtables := make([]*mvcc.MemTable, 1+len(workspace.immutableMemTables))
	allMemtables[0] = workspace.activeMemTable

	allMemtablesIndex := 1
	for index := len(workspace.immutableMemTables) - 1; index >= 0; index-- {
		allMemtables[allMemtablesIndex] = workspace.immutableMemTables[index]
		allMemtablesIndex = allMemtablesIndex + 1
	}

	return allMemtables
}

// RemoveAllWAL removes the WAL of all the memtables. It is ONLY used from tests.
func (workspace *Workspace) RemoveAllWAL() {
	workspace.activeMemTable.RemoveWAL()
	for _, memtable := range workspace.immutableMemTables {
		memtable.RemoveWAL()
	}
}
