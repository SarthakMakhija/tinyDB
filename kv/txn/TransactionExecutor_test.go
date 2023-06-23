package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/kv"
	"tinydb/kv/mvcc"
)

func TestExecutesABatch(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	noCallback := func() {}
	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, noCallback))
	<-doneChannel

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.ValueSlice())
}

func TestExecutesABatchAnInvokesCommitCallback(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))

	commitCallback := func() {
		_ = memTable.PutOrUpdate(mvcc.NewVersionedKey([]byte("commit"), 1), mvcc.NewValue([]byte("applied")))
	}
	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, commitCallback))
	<-doneChannel

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("commit"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("applied"), value.ValueSlice())
}

func TestExecutes2Batches(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	executor := NewTransactionExecutor(memTable)

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

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Serialized Snapshot"), value.ValueSlice())
}

func TestExecutesABatchAndStops(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	noCallback := func() {}

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1, noCallback))
	<-doneChannel

	executor.Stop()

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.ValueSlice())
}
