package sstable

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
)

func TestExistingKeyInAnSSTableBlock(t *testing.T) {
	builder := NewSSTableBuilder(option.DefaultOptions())
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 3), mvcc.NewValue([]byte("Hard disk drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("SSD"), 1), mvcc.NewValue([]byte("Solid state drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("Versioning"), 1), mvcc.NewValue([]byte("Semantic")))
	builder.finishBlock()

	blockIterator := NewBlockIterator(builder.currentBlock)
	blockIterator.Seek(mvcc.NewVersionedKey([]byte("HDD"), 2))

	assert.Equal(t, "HDD", blockIterator.key.AsString())
	assert.Equal(t, "Hard disk drive", string(blockIterator.value.ValueSlice()))
}

func TestNonExistingKeyInAnSSTableBlock(t *testing.T) {
	builder := NewSSTableBuilder(option.DefaultOptions())
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 3), mvcc.NewValue([]byte("Hard disk drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("SSD"), 1), mvcc.NewValue([]byte("Solid state drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("Versioning"), 1), mvcc.NewValue([]byte("Semantic")))
	builder.finishBlock()

	blockIterator := NewBlockIterator(builder.currentBlock)
	blockIterator.Seek(mvcc.NewVersionedKey([]byte("ZERO"), 1))

	assert.Error(t, blockIterator.err)
}

func TestAKeyGreaterOrEqualToTheOneBeingLookedForInAnSSTableBlock(t *testing.T) {
	builder := NewSSTableBuilder(option.DefaultOptions())
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))
	builder.Add(mvcc.NewVersionedKey([]byte("HDD"), 3), mvcc.NewValue([]byte("Hard disk drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("SSD"), 1), mvcc.NewValue([]byte("Solid state drive")))
	builder.Add(mvcc.NewVersionedKey([]byte("Versioning"), 1), mvcc.NewValue([]byte("Semantic")))
	builder.finishBlock()

	blockIterator := NewBlockIterator(builder.currentBlock)
	blockIterator.Seek(mvcc.NewVersionedKey([]byte("REQUEST"), 1))

	assert.Equal(t, "SSD", blockIterator.key.AsString())
	assert.Equal(t, "Solid state drive", string(blockIterator.value.ValueSlice()))
}
