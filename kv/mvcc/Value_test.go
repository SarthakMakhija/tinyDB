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
	encoded := value.encode()

	decodedValue := new(Value)
	decodedValue.decodeFrom(encoded)

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
	encoded := value.encode()

	decodedValue := new(Value)
	decodedValue.decodeFrom(encoded)

	assert.Equal(t, true, decodedValue.IsDeleted())
	assert.Equal(t, "", string(decodedValue.ValueSlice()))
}
