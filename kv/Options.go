package kv

type Options struct {
	DbDirectory string
}

func DefaultOptions() *Options {
	return &Options{}
}

func (options *Options) SetDbDirectory(dbDirectory string) *Options {
	options.DbDirectory = dbDirectory
	return options
}
