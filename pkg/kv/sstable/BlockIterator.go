package sstable

import (
	"io"
	"sort"
	"tinydb/pkg/kv/mvcc"
)

type BlockIterator struct {
	block *Block
	key   *mvcc.VersionedKey
	value *mvcc.Value
	err   error
}

func NewBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block: block,
	}
}

func (blockIterator *BlockIterator) Seek(key mvcc.VersionedKey) {
	totalEntriesInBlock := len(blockIterator.block.entryBeginOffsets)
	index := sort.Search(totalEntriesInBlock, func(index int) bool {
		blockIterator.initializeAt(index)
		return blockIterator.key.Compare(key) >= 0
	})
	blockIterator.initializeAt(index)
}

func (blockIterator *BlockIterator) initializeAt(index int) {
	if index >= len(blockIterator.block.entryBeginOffsets) || index < 0 {
		blockIterator.err = io.EOF
		return
	}
	getEntryHeader := func(entryBeginOffset uint32) *EntryHeader {
		entryHeader := new(EntryHeader)
		entryHeader.decodeFrom(blockIterator.block.buffer[entryBeginOffset:])

		return entryHeader
	}
	getKeyValueAsBytes := func(entryBeginOffset uint32, entryHeader *EntryHeader) []byte {
		return blockIterator.block.buffer[entryBeginOffset : entryBeginOffset+uint32(entryHeader.entrySize)]
	}
	getKey := func(keyValueBytes []byte, entryHeader *EntryHeader) *mvcc.VersionedKey {
		keyBytes := keyValueBytes[entryHeaderSize : entryHeaderSize+entryHeader.keySize]
		versionedKey := new(mvcc.VersionedKey)
		versionedKey.DecodeFrom(keyBytes)

		return versionedKey
	}
	getValue := func(keyValueBytes []byte, entryHeader *EntryHeader) *mvcc.Value {
		valueBytes := keyValueBytes[entryHeaderSize+entryHeader.keySize:]
		value := new(mvcc.Value)
		value.DecodeFrom(valueBytes)

		return value
	}
	entryBeginOffset := blockIterator.block.entryBeginOffsets[index]
	entryHeader := getEntryHeader(entryBeginOffset)
	keyValueBytes := getKeyValueAsBytes(entryBeginOffset, entryHeader)

	blockIterator.key = getKey(keyValueBytes, entryHeader)
	blockIterator.value = getValue(keyValueBytes, entryHeader)
}
