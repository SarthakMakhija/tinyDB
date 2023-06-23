# KV

## Memtable + WAL
- [X] Create a new WAL (segment) with every memtable
- [X] Write to WAL on memtable's `PutOrUpdate`
- [ ] Provide an option to perform SYNC after every batch write in WAL
- [ ] Close the WAL (segment) when the memtable is full
- [X] Delete in memtable

## Support for iterator
- [X] Iterator for Skiplist
  - [ ] Check if iterator can return a deleted key/value

## Prefix based get/seek
## Flush memtable to disk
## Creation of SSTable
## Bloom filter
## Recovery
