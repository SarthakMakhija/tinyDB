package mvcc

import (
	"sync"
	"tinydb/pkg/kv/mvcc/utils"
)

const MaxHeight = 20

type Skiplist struct {
	lock           sync.RWMutex
	head           *SkiplistNode
	levelGenerator utils.LevelGenerator
	size           uint64
}

func newSkiplist() *Skiplist {
	return &Skiplist{
		head:           newSkiplistNode(emptyVersionedKey(), emptyValue(), MaxHeight),
		levelGenerator: utils.NewLevelGenerator(MaxHeight),
		size:           0,
	}
}

// PutOrUpdate puts or updates the key and the value pair in the SkipList and increases the size of skiplist.
func (skiplist *Skiplist) putOrUpdate(key VersionedKey, value Value) {
	skiplist.lock.Lock()
	defer skiplist.lock.Unlock()

	skiplist.head.putOrUpdate(key, value, skiplist.levelGenerator)
	skiplist.size = skiplist.size + key.size() + value.size()
}

// Get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
func (skiplist *Skiplist) get(key VersionedKey) (Value, bool) {
	skiplist.lock.RLock()
	defer skiplist.lock.RUnlock()

	return skiplist.head.get(key)
}

// iterator returns an Iterator that allows forward movement in the Skiplist
func (skiplist *Skiplist) iterator() *Iterator {
	return &Iterator{
		skiplist: skiplist,
		node:     skiplist.head,
	}
}

// SkiplistNode represents a node in the SkipList.
// Each node contains the key/value pair and an array of forward pointers.
// SkipListNode maintains VersionedKeys: each key has a version which is the commitTimestamp.
// A sample Level0 of SkipListNode with HDD as the key can be represented as:
// HDD1: Hard Disk -> HDD2: Hard disk -> HDD5: Hard disk drive. Here, 1, 2, and 5 are the versions of the key HDD.
type SkiplistNode struct {
	key      VersionedKey
	value    Value
	forwards []*SkiplistNode
}

// newSkiplistNode creates a new instance of SkiplistNode.
func newSkiplistNode(key VersionedKey, value Value, level uint8) *SkiplistNode {
	return &SkiplistNode{
		key:      key,
		value:    value,
		forwards: make([]*SkiplistNode, level),
	}
}

// putOrUpdate puts or updates the value corresponding to the incoming key.
func (node *SkiplistNode) putOrUpdate(key VersionedKey, value Value, levelGenerator utils.LevelGenerator) bool {
	current := node
	positions := make([]*SkiplistNode, len(node.forwards))

	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && current.forwards[level].key.compare(key) < 0 {
			current = current.forwards[level]
		}
		positions[level] = current
	}

	current = current.forwards[0]

	//same version of the key must not be present
	if current == nil || current.key.compare(key) != 0 {
		newLevel := levelGenerator.Generate()
		newNode := newSkiplistNode(key, value, newLevel)
		for level := uint8(0); level < newLevel; level++ {
			newNode.forwards[level] = positions[level].forwards[level]
			positions[level].forwards[level] = newNode
		}
		return true
	}
	return false
}

// get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
// get attempts to find the key where:
// 1. the version of the key < version of the incoming key &&
// 2. the key prefixes match.
// KeyPrefix is the actual key or the byte slice.
func (node *SkiplistNode) get(key VersionedKey) (Value, bool) {
	node, ok := node.matchingNode(key)
	if ok && !node.value.IsDeleted() {
		return node.value, true
	}
	return emptyValue(), false
}

func (node *SkiplistNode) matchingNode(key VersionedKey) (*SkiplistNode, bool) {
	current := node
	lastNodeWithTheKey := current
	for level := len(node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && current.forwards[level].key.compare(key) < 0 {
			current = current.forwards[level]
			lastNodeWithTheKey = current
		}
	}
	if current != nil && current.key.matchesKeyPrefix(key.getKey()) {
		return current, true
	}
	if lastNodeWithTheKey != nil && lastNodeWithTheKey.key.matchesKeyPrefix(key.getKey()) {
		return lastNodeWithTheKey, true
	}
	return nil, false
}

// Iterator allows forward movement in the Skiplist
type Iterator struct {
	skiplist *Skiplist
	node     *SkiplistNode
}

// seek to a node such that node.key >= key
func (iterator *Iterator) seek(key VersionedKey) {
	iterator.skiplist.lock.RLock()
	defer iterator.skiplist.lock.RUnlock()

	current := iterator.node
	for level := len(iterator.node.forwards) - 1; level >= 0; level-- {
		for current.forwards[level] != nil && current.forwards[level].key.compare(key) <= 0 {
			current = current.forwards[level]
		}
	}
	if current.key.compare(key) < 0 {
		current = current.forwards[0]
	}
	iterator.node = current
}

// isValid returns true if the current iterator node is not nil, false otherwise
func (iterator *Iterator) isValid() bool {
	return iterator.node != nil
}

// key returns the key present in the current node pointed to by the Iterator
func (iterator *Iterator) key() VersionedKey {
	return iterator.node.key
}

// value returns the value present in the current node pointed to by the Iterator
func (iterator *Iterator) value() Value {
	return iterator.node.value
}

// next moves the iterator forward. It is ESSENTIAL to call isValid() before calling next.
// No nil check is done on the iterator node. It is the responsibility of the callee to ensure next is only called if the
// Iterator is valid
func (iterator *Iterator) next() {
	iterator.skiplist.lock.RLock()
	defer iterator.skiplist.lock.RUnlock()

	current := iterator.node
	current = current.forwards[0]
	iterator.node = current
}
