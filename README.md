# tinyDB (WIP -> Dropped)
Tiny relational DB implementation over an LSM tree based storage engine. I am not working on this anymore. The project has moved to [gorel](https://github.com/SarthakMakhija/gorel).

# Idea

The idea is to provide a relational database implementation over an LSM tree based storage engine. The implementation will be used alongside my [storage engine workshop](https://github.com/SarthakMakhija/storage-engine-workshop-template).
It will support the following:

**LSM tree based KV storage engine**

- [ ] Persistence
- [ ] Recovery
- [ ] Support for `put(key, value)`
- [ ] Support for `update(key, value)`
- [ ] Support for `get(key)`
- [ ] Support for `delete(key)`
- [ ] Support for the getting all the values by a key prefix
- [ ] Serialized snapshot transaction isolation
- [ ] Concurrent execution
- [ ] (Optional) Compaction

**Relational database**

- [ ] Support for creating tables
- [ ] Support for column data types: INT, STRING, FLOAT
- [ ] Support for creating primary and secondary key
- [ ] Support for INSERT statement
- [ ] Support for SELECT query by the primary key 
- [ ] Support for SELECT query by the secondary key 
- [ ] Support for SELECT query by non-key columns
- [ ] Query parsing using [goyacc](https://pkg.go.dev/golang.org/x/tools/cmd/goyacc): CREATE TABLE, INSERT INTO, SELECT
