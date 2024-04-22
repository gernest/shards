package shards

import "fmt"

type Set struct {
	shardsMap map[uint64]struct{}
	shardsVer int64 // increment with each change.

	// give out readonly to repeated consumers if
	// readonlyVer == shardsVer
	readonly    map[uint64]struct{}
	readonlyVer int64
}

func (a *Set) UnionInPlace(b *Set) {
	shards := b.CloneMaybe()
	for shard := range shards {
		a.Add(shard)
	}
}

func (a *Set) Equals(b *Set) bool {
	if len(a.shardsMap) != len(b.shardsMap) {
		return false
	}
	for shardInA := range a.shardsMap {
		_, ok := b.shardsMap[shardInA]
		if !ok {
			return false
		}
	}
	return true

}

func (a *Set) Shards() []uint64 {
	s := make([]uint64, 0, len(a.shardsMap))
	for si := range a.shardsMap {
		s = append(s, si)
	}
	return s
}

func (ss *Set) String() (r string) {
	r = "["
	for k := range ss.shardsMap {
		r += fmt.Sprintf("%v, ", k)
	}
	r += "]"
	return
}

func (ss *Set) Add(shard uint64) {
	_, already := ss.shardsMap[shard]
	if !already {
		ss.shardsMap[shard] = struct{}{}
		ss.shardsVer++
	}
}

// CloneMaybe maintains a re-usable readonly version
// ss.shards that can be returned to multiple goroutine
// reads as it will never change. A copy is only made
// once for each change in the shard set.
func (ss *Set) CloneMaybe() map[uint64]struct{} {

	if ss.readonlyVer == ss.shardsVer {
		return ss.readonly
	}

	// readonlyVer is out of date.
	// readonly needs update. We cannot
	// modify the readonly map in place;
	// must make a fully new copy here.
	ss.readonly = make(map[uint64]struct{})

	for k := range ss.shardsMap {
		ss.readonly[k] = struct{}{}
	}
	ss.readonlyVer = ss.shardsVer
	return ss.readonly
}

func NewSet() *Set {
	return &Set{
		shardsMap: make(map[uint64]struct{}),
	}
}
