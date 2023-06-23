package mvcc

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const versionSize = int(unsafe.Sizeof(uint64(0)))

// VersionedKey represents a key with a Version.
// Versioned is used as a key inside Skiplist based memtable which acts as an in-memory store.
// Versioned key has a Version field that is the commitTimestamp of the key which is assigned by txn.Oracle.
type VersionedKey struct {
	key     []byte
	Version uint64
}

// NewVersionedKey creates a new instance of the VersionedKey
func NewVersionedKey(key []byte, version uint64) VersionedKey {
	return VersionedKey{key: key, Version: version}
}

// emptyVersionedKey creates an empty VersionedKey.
// This is used to create the sentinel node of Skiplist.
func emptyVersionedKey() VersionedKey {
	return VersionedKey{}
}

// getKey returns the key from the VersionedKey
func (versionedKey VersionedKey) getKey() []byte {
	return versionedKey.key
}

// getVersion returns the Version from the VersionedKey
func (versionedKey VersionedKey) getVersion() uint64 {
	return versionedKey.Version
}

// compare the two VersionedKeys.
// Two VersionedKeys are equal if their contents and the versions are same.
// If two VersionedKeys are equal in their content, then their Version is used to
// get the comparison result.
func (versionedKey VersionedKey) compare(other VersionedKey) int {
	comparisonResult := bytes.Compare(versionedKey.getKey(), other.getKey())
	if comparisonResult == 0 {
		thisVersion, otherVersion := versionedKey.getVersion(), other.getVersion()
		if thisVersion == otherVersion {
			return 0
		}
		if thisVersion < otherVersion {
			return -1
		}
		return 1
	}
	return comparisonResult
}

// matchesKeyPrefix returns true if the key part of the VersionedKey matches the incoming key.
func (versionedKey VersionedKey) matchesKeyPrefix(key []byte) bool {
	return bytes.Compare(versionedKey.getKey(), key) == 0
}

// asString returns the string of the key part.
func (versionedKey VersionedKey) asString() string {
	return string(versionedKey.key)
}

// encode the VersionedKey
// Encoding scheme: [<Key>|<Version 8 bytes>] in a byte slice.
func (versionedKey VersionedKey) encode() []byte {
	encoded := make([]byte, len(versionedKey.key)+versionSize)
	binary.LittleEndian.PutUint64(encoded[:], versionedKey.Version)
	copy(encoded[versionSize:], versionedKey.key)
	return encoded
}

// decode the incoming byte slice and mutate the versionedKey with Version and the key.
func (versionedKey *VersionedKey) decode(part []byte) {
	version := binary.LittleEndian.Uint64(part)
	key := part[versionSize:]

	versionedKey.Version = version
	versionedKey.key = key
}

// size returns the total size of a single VersionedKey
func (versionedKey *VersionedKey) size() uint64 {
	return uint64(len(versionedKey.key) + versionSize)
}
