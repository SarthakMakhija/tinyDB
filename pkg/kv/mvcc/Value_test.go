package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNonDeletedValue(t *testing.T) {
	value := NewValue([]byte("Hard disk"))
	assert.Equal(t, "Hard disk", string(value.ValueSlice()))
	assert.Equal(t, false, value.IsDeleted())
}

func TestNonDeletedEncodedValue(t *testing.T) {
	value := NewValue([]byte("Hard disk"))
	encoded := value.Encode()

	decodedValue := new(Value)
	decodedValue.DecodeFrom(encoded)

	assert.Equal(t, false, decodedValue.IsDeleted())
	assert.Equal(t, "Hard disk", string(decodedValue.ValueSlice()))
}

func TestDeletedValue(t *testing.T) {
	value := NewDeletedValue()
	assert.Equal(t, "", string(value.ValueSlice()))
	assert.Equal(t, true, value.IsDeleted())
}

func TestDeletedEncodedValue(t *testing.T) {
	value := NewDeletedValue()
	encoded := value.Encode()

	decodedValue := new(Value)
	decodedValue.DecodeFrom(encoded)

	assert.Equal(t, true, decodedValue.IsDeleted())
	assert.Equal(t, "", string(decodedValue.ValueSlice()))
}

func TestValueSize(t *testing.T) {
	value := NewValue([]byte("Hard disk"))
	assert.Equal(t, uint64(10), value.size())
}

func TestDeletedValueSize(t *testing.T) {
	value := NewDeletedValue()
	assert.Equal(t, uint64(1), value.size())
}
