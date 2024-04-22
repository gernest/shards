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

type FieldView struct {
	// field -> view -> *shardSet
	m map[uint64]map[string]*Set
}

func (vs *FieldView) Get(field uint64) map[string]*Set {
	return vs.m[field]
}

func (vs *FieldView) AddSet(field uint64, view string, ss *Set) {

	f, ok := vs.m[field]
	if !ok {
		f = make(map[string]*Set)
		vs.m[field] = f
	}
	// INVAR: f is ready to take ss.

	// existing stuff to merge with?
	prior, ok := f[view]
	if !ok {
		f[view] = ss
		return
	}
	// merge ss and prior. No need to put the union back into f[fv.View]
	// because prior is a pointer.
	prior.UnionInPlace(ss)
}

func (a *FieldView) Equals(b *FieldView) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.m) != len(b.m) {
		return false
	}
	for field, viewmapA := range a.m {
		viewmapB, ok := b.m[field]
		if !ok {
			return false
		}
		if len(viewmapB) != len(viewmapA) {
			return false
		}
		for k, va := range viewmapA {
			vb, ok := viewmapB[k]
			if !ok {
				return false
			}
			if !va.Equals(vb) {
				return false
			}
		}
	}
	return true
}

func NewFieldView() *FieldView {
	return &FieldView{
		m: make(map[uint64]map[string]*Set), // expected response from GetView2ShardMapForIndex
	}
}

func (vs *FieldView) AddShard(field uint64, view string, shard uint64) {
	viewmap, ok := vs.m[field]
	if !ok {
		viewmap = make(map[string]*Set)
		vs.m[field] = viewmap
	}
	ss, ok := viewmap[view]
	if !ok {
		ss = NewSet()
		viewmap[view] = ss
	}
	ss.Add(shard)
}

func (vs *FieldView) String() (r string) {
	r = "\n"
	for field, viewmap := range vs.m {
		for view, shards := range viewmap {
			r += fmt.Sprintf("field '%v' view:'%v' shards:%v\n", field, view, shards)
		}
	}
	r += "\n"
	return
}

func (vs *FieldView) Remove(field uint64) {
	delete(vs.m, field)
}
