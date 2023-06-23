package mvcc

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"tinydb/kv"
)

func RandomWALFileId() uint64 {
	return rand.Uint64()
}

func TestPutsAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))
	_ = memTable.PutOrUpdate(key, value)

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 2))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())
}

func TestPutsTheSameKeyWithADifferentVersionInMemTable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 3))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersionInMemTable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 8))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())
}

func TestGetsTheValueOfANonExistingKeyInMemTable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	_, ok := memTable.Get(NewVersionedKey([]byte("Storage"), 1))

	assert.Equal(t, false, ok)
}

func TestUpdatesAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())
}

func TestPutsKeysValuesConcurrentlyInMemtable(t *testing.T) {
	memTable, _ := NewMemTable(RandomWALFileId(), kv.DefaultOptions())
	defer memTable.RemoveWAL()

	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	}()
	go func() {
		defer wg.Done()
		_ = memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))
	}()
	go func() {
		defer wg.Done()
		_ = memTable.PutOrUpdate(NewVersionedKey([]byte("SSD"), 1), NewValue([]byte("Solid state")))
	}()

	wg.Wait()

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.ValueSlice())

	value, ok = memTable.Get(NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.ValueSlice())

	value, ok = memTable.Get(NewVersionedKey([]byte("SSD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Solid state"), value.ValueSlice())
}
