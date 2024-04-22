package shards

type Holder struct{}

type Index struct {
	Shard map[uint64]*Shard
}

type Shard struct {
	HolderPath string
	Shard      uint64
	Open       bool
}

type DB struct{}
