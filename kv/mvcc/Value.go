package mvcc

var nilValue []byte

// Value wraps a []byte which acts as a value in the MemTable.
type Value struct {
	value   []byte
	deleted byte
}

// NewValue creates a new instance of the Value.
func NewValue(value []byte) Value {
	return Value{
		value:   value,
		deleted: byte(0),
	}
}

// NewDeletedValue creates a new instance of the Value with deleted flag.
func NewDeletedValue() Value {
	return Value{
		value:   nilValue,
		deleted: byte(1),
	}
}

// emptyValue returns an empty Value. Is used when the value for a key is not found.
func emptyValue() Value {
	return Value{}
}

// ValueSlice returns the byte slice present in the Value.
func (value Value) ValueSlice() []byte {
	return value.value
}

// IsDeleted returns true if the value is deleted, false otherwise
func (value Value) IsDeleted() bool {
	return value.deleted&0x01 == 0x01
}

// IsDeleted returns true if the value is deleted, false otherwise
func (value Value) encode() []byte {
	encoded := make([]byte, len(value.ValueSlice())+1)
	copy(encoded[:1], []byte{value.deleted})
	copy(encoded[1:], value.value)

	return encoded
}

// decodeFrom sets the deleted and value from the byte slice
func (value *Value) decodeFrom(part []byte) {
	value.deleted = part[0]
	value.value = part[1:]
}
