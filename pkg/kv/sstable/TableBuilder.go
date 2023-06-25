package sstable

import (
	"encoding/binary"
	"tinydb/pkg/kv/mvcc"
	"tinydb/pkg/kv/option"
	"tinydb/pkg/kv/utils"
	"unsafe"
)

const uint16Size = int(unsafe.Sizeof(uint16(0)))
const entryHeaderSize = uint16(unsafe.Sizeof(EntryHeader{}))

type TableBuilder struct {
	options      *option.Options
	currentBlock *Block
}

//Structure of an entry.
/*
+-------------------+------------------+-----+-------+
| 2 bytes entrySize | 2 bytes key size | key | Value |
+-------------------+------------------+-----+-------+
*/

// Block
/*
Structure of a Block.
+-------------------+---------------------+--------------------+
| Entry1            | Entry2              | Entry3             |
+-------------------+---------------------+--------------------+
| List of offsets used by each key        | Block Meta Size    |
| present in the data block               | (4 Bytes)          |
+-----------------------------------------+--------------------+
*/
type Block struct {
	firstKey          []byte
	buffer            []byte
	entryBeginOffsets []uint32
	endOffset         int
}

type EntryHeader struct {
	entrySize uint16
	keySize   uint16
}

func NewSSTableBuilder(options *option.Options) *TableBuilder {
	builder := &TableBuilder{options: options}
	builder.currentBlock = builder.newBlock()
	return builder
}

// Add
// TODO: Index block
// TODO: Footer block
func (builder *TableBuilder) Add(key mvcc.VersionedKey, value mvcc.Value) {
	encodedKey, encodedValue := key.Encode(), value.Encode()
	if builder.currentBlock.firstKey == nil {
		builder.currentBlock.firstKey = encodedKey
	}
	builder.currentBlock.entryBeginOffsets = append(builder.currentBlock.entryBeginOffsets, uint32(builder.currentBlock.endOffset))
	builder.append(newEntryHeader(encodedKey, encodedValue).encode())
	builder.append(encodedKey)
	builder.append(encodedValue)
}

func (builder *TableBuilder) finishBlock() {
	builder.append(utils.U32SliceToBytes(builder.currentBlock.entryBeginOffsets))
	builder.append(utils.U32ToBytesLittleEndian(uint32(len(builder.currentBlock.entryBeginOffsets))))
}

func (builder *TableBuilder) append(part []byte) {
	destination := builder.allocate(len(part))
	copy(destination, part)
}

func (builder *TableBuilder) allocate(space int) []byte {
	//TODO: Does allocate need resizing?
	currentBlock := builder.currentBlock
	currentBlock.endOffset = currentBlock.endOffset + space
	return currentBlock.buffer[currentBlock.endOffset-space : currentBlock.endOffset]
}

func (builder *TableBuilder) newBlock() *Block {
	return &Block{
		buffer:            make([]byte, builder.options.SSTableBlockSizeInBytes),
		entryBeginOffsets: []uint32{},
	}
}

func newEntryHeader(key []byte, value []byte) *EntryHeader {
	return &EntryHeader{
		entrySize: uint16(len(key)) + uint16(len(value)) + entryHeaderSize,
		keySize:   uint16(len(key)),
	}
}

func (entryHeader EntryHeader) encode() []byte {
	bytes := make([]byte, entryHeaderSize)
	binary.LittleEndian.PutUint16(bytes, entryHeader.entrySize)
	binary.LittleEndian.PutUint16(bytes[uint16Size:], entryHeader.keySize)

	return bytes
}

func (entryHeader *EntryHeader) decodeFrom(part []byte) {
	entryHeader.entrySize = binary.LittleEndian.Uint16(part)
	entryHeader.keySize = binary.LittleEndian.Uint16(part[uint16Size:])
}
