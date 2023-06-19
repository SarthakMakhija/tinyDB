# KV

## Memtable + WAL
1. Create a new WAL (segment) with every memtable
2. Write to WAL on memtable's `PutOrUpdate`
   1. [Confirm]: if multiple KV pairs are written as one byte slice
3. Provide an option to perform SYNC after every batch write in WAL
4. Close the WAL (segment) when the memtable is full

## Support for iterator
## Prefix based get/seek
## Flush memtable to disk
## Creation of SSTable
## Bloom filter
## Recovery
