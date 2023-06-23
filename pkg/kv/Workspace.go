package kv

import (
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
)

// Workspace
// TODO: Check if we need any locks (during get), because put/delete will happen serially on the commit of a transaction;
// TODO: but get can run concurrently.
type Workspace struct {
	activeMemTable     *mvcc.MemTable
	immutableMemTables []*mvcc.MemTable
	options            *option.Options
}

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

func (workspace *Workspace) PutOrUpdate(key mvcc.VersionedKey, value mvcc.Value) error {
	if err := workspace.ensureRoom(); err != nil {
		return err
	}
	return workspace.activeMemTable.PutOrUpdate(key, value)
}

func (workspace *Workspace) Delete(key mvcc.VersionedKey) error {
	if err := workspace.ensureRoom(); err != nil {
		return err
	}
	return workspace.activeMemTable.Delete(key)
}

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

func (workspace *Workspace) removeAllWAL() {
	workspace.activeMemTable.RemoveWAL()
	for _, memtable := range workspace.immutableMemTables {
		memtable.RemoveWAL()
	}
}
