package option

type Options struct {
	DbDirectory             string
	MemtableSizeInBytes     uint64
	SSTableBlockSizeInBytes uint32
}

func DefaultOptions() *Options {
	return &Options{
		MemtableSizeInBytes:     32 * 1024 * 1024,
		SSTableBlockSizeInBytes: 4096,
	}
}

func (options *Options) SetDbDirectory(dbDirectory string) *Options {
	options.DbDirectory = dbDirectory
	return options
}

func (options *Options) SetMemtableSizeInBytes(memtableSize uint64) *Options {
	options.MemtableSizeInBytes = memtableSize
	return options
}
