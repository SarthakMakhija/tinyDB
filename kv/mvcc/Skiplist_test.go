package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	skiplist := newSkiplist()

	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))

	skiplist.putOrUpdate(key, value)

	value, ok := skiplist.get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())
}

func TestPutsADeletedKeyValueAndGetByKeyInNode(t *testing.T) {
	skiplist := newSkiplist()

	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewDeletedValue()

	skiplist.putOrUpdate(key, value)

	_, ok := skiplist.get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, false, ok)
}

func TestUpdatesTheSameKeyWithADifferentVersion(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := skiplist.get(NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersion(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := skiplist.get(NewVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())
}

func TestGetsTheValueOfAKeyWithLatestVersion(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 1), NewValue([]byte("Solid state drive")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid State drive")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 3), NewValue([]byte("Solid-State-drive")))

	expected := make(map[uint64][]byte)
	expected[2] = []byte("Solid state drive")
	expected[3] = []byte("Solid State drive")
	expected[4] = []byte("Solid-State-drive")
	expected[5] = []byte("Solid-State-drive")

	for version, expectedValue := range expected {
		key := NewVersionedKey([]byte("SSD"), version)
		value, ok := skiplist.get(key)

		assert.Equal(t, true, ok)
		assert.Equal(t, expectedValue, value.ValueSlice())
	}
}

func TestGetsTheValueForNonExistingKey(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	_, ok := skiplist.get(NewVersionedKey([]byte("Storage"), 1))
	assert.Equal(t, false, ok)
}

func TestIteratorSeekWithMatchingKey(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid state")))

	iterator := skiplist.iterator()
	iterator.seek(NewVersionedKey([]byte("SSD"), 2))

	assert.True(t, iterator.isValid())
	assert.Equal(t, "SSD", iterator.key().asString())
	assert.Equal(t, "Solid state", string(iterator.value().ValueSlice()))
}

func TestIteratorSeekWithKeyGreaterThanTheExistingKey(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid state")))

	iterator := skiplist.iterator()
	iterator.seek(NewVersionedKey([]byte("SSD"), 1))

	assert.True(t, iterator.isValid())
	assert.Equal(t, "SSD", iterator.key().asString())
	assert.Equal(t, "Solid state", string(iterator.value().ValueSlice()))
}

func TestIteratorSeekWithKeyDifferentThanKeyPrefix(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("DB"), 1), NewValue([]byte("TinyDB")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid state")))

	iterator := skiplist.iterator()
	iterator.seek(NewVersionedKey([]byte("DB"), 2))

	assert.True(t, iterator.isValid())
	assert.Equal(t, "HDD", iterator.key().asString())
	assert.Equal(t, "Hard disk", string(iterator.value().ValueSlice()))
}

func TestIteratorNext(t *testing.T) {
	skiplist := newSkiplist()

	skiplist.putOrUpdate(NewVersionedKey([]byte("DB"), 1), NewValue([]byte("TinyDB")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	skiplist.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid state")))

	iterator := skiplist.iterator()
	iterator.seek(NewVersionedKey([]byte("DB"), 2))

	assert.True(t, iterator.isValid())
	assert.Equal(t, "HDD", iterator.key().asString())
	assert.Equal(t, "Hard disk", string(iterator.value().ValueSlice()))

	iterator.next()
	assert.True(t, iterator.isValid())
	assert.Equal(t, "SSD", iterator.key().asString())
	assert.Equal(t, "Solid state", string(iterator.value().ValueSlice()))

	iterator.next()
	assert.False(t, iterator.isValid())
}
