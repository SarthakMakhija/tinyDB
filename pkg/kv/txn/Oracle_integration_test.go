package txn

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
	"tinydb/pkg/kv"
	"tinydb/pkg/kv/option"
)

func RandomWALFileId() uint64 {
	return rand.Uint64()
}

func TestBeginTimestampMarkWithASingleTransaction(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))
	transaction.FinishBeginTimestampForReadWriteTransaction()

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), beginMark.DoneTill())
}

func TestBeginTimestampMarkWithTwoTransactions(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)

	oracle.commitTimestampMark.Finish(commitTimestamp)
	transaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	anotherTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())
}

func TestCleanUpOfCommittedTransactions(t *testing.T) {
	workspace, _ := kv.NewWorkspace(option.DefaultOptions().SetDbDirectory("."))
	defer workspace.RemoveAllWAL()

	oracle := NewOracle(NewTransactionExecutor(workspace))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	transaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	anotherTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())

	thirdTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(thirdTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	thirdTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(3), commitTimestamp)

	time.Sleep(15 * time.Millisecond)
	assert.Equal(t, uint64(2), beginMark.DoneTill())

	committedTransactions := oracle.committedTransactions
	assert.Equal(t, 1, len(committedTransactions))
	assert.Equal(t, uint64(3), committedTransactions[0].commitTimestamp)
}
