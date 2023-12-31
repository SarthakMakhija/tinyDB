package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/pkg/kv"
	"tinydb/pkg/kv/option"
	"tinydb/pkg/kv/txn/errors"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))
	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterACommit(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, uint64(1), oracle.beginTimestamp())
}

func TestGetsCommitTimestampForTransactionGivenNoTransactionsAreCurrentlyTracked(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)
}

func TestGetsCommitTimestampFor2Transactions(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	aTransaction := NewReadWriteTransaction(oracle)
	aTransaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("SSD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestGetsCommitTimestampFor2TransactionsGivenOneTransactionReadTheKeyThatTheOtherWrites(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	aTransaction := NewReadWriteTransaction(oracle)
	_ = aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestErrorsForOneTransaction(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	aTransaction := NewReadWriteTransaction(oracle)
	_ = aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	_ = anotherTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))
	anotherTransaction.Get([]byte("HDD"))

	thirdTransaction := NewReadWriteTransaction(oracle)
	thirdTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(2), commitTimestamp)

	_, err := oracle.mayBeCommitTimestampFor(thirdTransaction)
	assert.Error(t, err)
	assert.Equal(t, errors.ConflictErr, err)
}
