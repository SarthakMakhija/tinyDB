package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/pkg/kv"
	mvcc "tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/txn/errors"
)

func TestGetsANonExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	transaction := NewReadonlyTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_, ok := transaction.Get([]byte("non-existing"))

	assert.Equal(t, false, ok)
}

func TestGetsAnExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	_ = memTable.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))

	oracle := NewOracle(NewTransactionExecutor(memTable))
	oracle.nextTimestamp = 3

	oracle.commitTimestampMark.Finish(2)

	transaction := NewReadonlyTransaction(oracle)
	valueWithVersion, ok := transaction.Get([]byte("HDD"))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())
}

func TestCommitsAnEmptyReadWriteTransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	oracle := NewOracle(NewTransactionExecutor(memTable))
	oracle.commitTimestampMark.Finish(2)

	transaction := NewReadWriteTransaction(oracle)

	_, err := transaction.Commit()

	assert.Error(t, err)
	assert.Equal(t, errors.EmptyTransactionErr, err)
}

func TestAttemptsToPutDuplicateKeysInATransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	oracle := NewOracle(NewTransactionExecutor(memTable))
	oracle.commitTimestampMark.Finish(2)

	transaction := NewReadWriteTransaction(oracle)

	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	err := transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))

	assert.Error(t, err)
	assert.Equal(t, errors.DuplicateKeyInBatchErr, err)
}

func TestGetsAnExistingKeyInAReadWriteTransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	oracle := NewOracle(NewTransactionExecutor(memTable))

	transaction := NewReadWriteTransaction(oracle)
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	done, _ := transaction.Commit()
	<-done

	anotherTransaction := NewReadWriteTransaction(oracle)
	_ = anotherTransaction.PutOrUpdate([]byte("SSD"), []byte("Solid state disk"))
	done, _ = transaction.Commit()
	<-done

	readonlyTransaction := NewReadonlyTransaction(oracle)

	valueWithVersion, ok := readonlyTransaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), valueWithVersion.ValueSlice())

	_, ok = readonlyTransaction.Get([]byte("SSD"))
	assert.Equal(t, false, ok)

	_, ok = readonlyTransaction.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestGetsTheValueFromAKeyInAReadWriteTransactionFromBatch(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	value, ok := transaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, transaction.beginTimestamp, value.Version)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	done, _ := transaction.Commit()
	<-done
}

func TestTracksReadsInAReadWriteTransaction(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("SSD"))

	done, _ := transaction.Commit()
	<-done

	assert.Equal(t, 1, len(transaction.reads))
	key := transaction.reads[0]

	assert.Equal(t, []byte("SSD"), key)
}

func TestDoesNotTrackReadsInAReadWriteTransactionIfKeysAreReadFromTheBatch(t *testing.T) {
	memTable, _ := mvcc.NewMemTable(RandomWALFileId(), kv.DefaultOptions().SetDbDirectory("."))
	defer memTable.RemoveWAL()

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("HDD"))

	done, _ := transaction.Commit()
	<-done

	assert.Equal(t, 0, len(transaction.reads))
}