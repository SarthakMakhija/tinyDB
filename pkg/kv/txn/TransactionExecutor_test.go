package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/pkg/kv"
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
)

func TestExecutesABatch(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	executor := NewTransactionExecutor(workspace)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	noCallback := func() {}
	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, noCallback))
	<-doneChannel

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("isolation"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), valueWithVersion.ValueSlice())
}

func TestExecutesABatchAnInvokesCommitCallback(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	executor := NewTransactionExecutor(workspace)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))

	commitCallback := func() {
		_ = workspace.PutOrUpdate(mvcc.NewVersionedKey([]byte("commit"), 1), mvcc.NewValue([]byte("applied")))
	}
	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, commitCallback))
	<-doneChannel

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("commit"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("applied"), valueWithVersion.ValueSlice())
}

func TestExecutes2Batches(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	executor := NewTransactionExecutor(workspace)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	noCallback := func() {}

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, noCallback))
	<-doneChannel

	anotherBatch := NewBatch()
	_ = anotherBatch.Add([]byte("HDD"), []byte("Hard disk drive"))
	_ = anotherBatch.Add([]byte("isolation"), []byte("Serialized Snapshot"))

	doneChannel = executor.Submit(anotherBatch.ToTimestampedBatch(2, noCallback))
	<-doneChannel

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("isolation"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("isolation"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Serialized Snapshot"), valueWithVersion.ValueSlice())
}

func TestExecutesABatchAndStops(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	executor := NewTransactionExecutor(workspace)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	noCallback := func() {}

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, noCallback))
	<-doneChannel

	executor.Stop()

	valueWithVersion, ok := workspace.Get(mvcc.NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())

	valueWithVersion, ok = workspace.Get(mvcc.NewVersionedKey([]byte("isolation"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), valueWithVersion.ValueSlice())
}
