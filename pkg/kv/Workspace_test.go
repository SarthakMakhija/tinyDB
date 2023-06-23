package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
)

func TestWorkspacePutAndGet(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 2), mvcc.NewValue([]byte("Hard disk drive")))

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk drive", string(valueWithVersion.ValueSlice()))
}

func TestWorkspaceGetTheValueWithTheNearestVersion(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 2), mvcc.NewValue([]byte("Hard disk drive")))

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk drive", string(valueWithVersion.ValueSlice()))
}

func TestWorkspaceGetANonExistingKey(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))

	_, ok := workspace.Get(mvcc.NewVersionedKey([]byte("SSD"), 1))
	assert.Equal(t, false, ok)
}

func TestWorkspaceDeleteAndGetADeletedKey(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 2), mvcc.NewValue([]byte("Hard disk drive")))
	_ = workspace.Delete(mvcc.NewVersionedKey([]byte("HDD"), 3))

	_, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, false, ok)
}

func TestWorkspaceDeleteAndGetAKeyWithADifferentVersion(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 2), mvcc.NewValue([]byte("Hard disk drive")))
	_ = workspace.Delete(mvcc.NewVersionedKey([]byte("HDD"), 2))

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk drive", string(valueWithVersion.ValueSlice()))
}

func TestMemtableFull(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory(".").SetMemtableSizeInBytes(20))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk drive")))

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk drive", string(valueWithVersion.ValueSlice()))

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 2), mvcc.NewValue([]byte("Hard disk")))
	assert.Equal(t, 1, len(workspace.immutableMemTables))
}

func TestWorkspaceGetAcrossAllTheMemtables(t *testing.T) {
	workspace, _ := NewWorkspace(option.DefaultOptions().SetDbDirectory(".").SetMemtableSizeInBytes(20))
	defer workspace.RemoveAllWAL()

	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk drive")))
	_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("SSD"), 1), mvcc.NewValue([]byte("Solid state drive")))

	assert.Equal(t, 1, len(workspace.immutableMemTables))

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Hard disk drive", string(valueWithVersion.ValueSlice()))

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("SSD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, "Solid state drive", string(valueWithVersion.ValueSlice()))
}
