package shards

import (
	"fmt"
	"io"
	"math"

	"github.com/gernest/rbf"
	txkey "github.com/gernest/rbf/short_txkey"
	"github.com/gernest/roaring"
	"github.com/pkg/errors"
)

type Tx struct {
	tx *rbf.Tx
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() {
	tx.tx.Rollback()
}

func (tx *Tx) RoaringBitmap(key Key) (*roaring.Bitmap, error) {
	return tx.tx.RoaringBitmap(key.Prefix())
}

func (tx *Tx) Container(key Key, containerKey uint64) (*roaring.Container, error) {
	return tx.tx.Container(key.Prefix(), containerKey)
}

func (tx *Tx) PutContainer(key Key, containerKey uint64, c *roaring.Container) error {
	return tx.tx.PutContainer(key.Prefix(), containerKey, c)
}

func (tx *Tx) RemoveContainer(key Key, containerKey uint64) error {
	return tx.tx.RemoveContainer(key.Prefix(), containerKey)
}

func (tx *Tx) Remove(key Key, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(key, true, a...)
}

func (tx *Tx) Contains(key Key, v uint64) (exists bool, err error) {
	return tx.tx.Contains(key.Prefix(), v)
}

func (tx *Tx) ContainerIterator(key Key, container uint64) (citer roaring.ContainerIterator, found bool, err error) {
	return tx.tx.ContainerIterator(key.Prefix(), container)
}

func (tx *Tx) Count(key Key) (uint64, error) {
	return tx.tx.Count(key.Prefix())
}

func (tx *Tx) Max(key Key) (uint64, error) {
	return tx.tx.Max(key.Prefix())
}

func (tx *Tx) Min(key Key) (uint64, bool, error) {
	return tx.tx.Min(key.Prefix())
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *Tx) CountRange(key Key, start, end uint64) (n uint64, err error) {
	return tx.tx.CountRange(key.Prefix(), start, end)
}

func (tx *Tx) OffsetRange(key Key, offset, start, end uint64) (*roaring.Bitmap, error) {
	return tx.tx.OffsetRange(key.Prefix(), offset, start, end)
}

func (tx *Tx) ImportRoaringBits(key Key, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	return tx.tx.ImportRoaringBits(key.Prefix(), rit, clear, log, rowSize)
}

func (tx *Tx) ApplyFilter(key Key, ckey uint64, filter roaring.BitmapFilter) (err error) {
	err = tx.tx.ApplyFilter(key.Prefix(), ckey, filter)
	return errors.Wrap(err, fmt.Sprintf("applying  filter for index %v", key))
}

func (tx *Tx) ApplyRewriter(key Key, ckey uint64, filter roaring.BitmapRewriter) (err error) {
	err = tx.tx.ApplyRewriter(key.Prefix(), ckey, filter)
	return errors.Wrap(err, fmt.Sprintf("applying rewriter for %v", key))
}

func (tx *Tx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {
	return tx.tx.GetSortedFieldViewList()
}

func (tx *Tx) GetFieldSizeBytes(field uint64) (uint64, error) {
	return tx.tx.GetSizeBytesWithPrefix(Key{Field: field}.FieldPrefix())
}

// SnapshotReader returns a reader that provides a snapshot of the current database.
func (tx *Tx) SnapshotReader() (io.Reader, error) {
	return tx.tx.SnapshotReader()
}

// Removed clears the specified bits and tells you which ones it actually removed.
func (tx *Tx) Removed(key Key, a ...uint64) (changed []uint64, err error) {
	if len(a) == 0 {
		return a, nil
	}
	name := key.Prefix()
	// this special case can/should possibly go away, except that it
	// turns out to be by far the most common case, and we need to know
	// there's at least two items to simplify the check-sorted thing.
	if len(a) == 1 {
		hi, lo := highbits(a[0]), lowbits(a[0])
		rc, err := tx.tx.Container(name, hi)
		if err != nil {
			return a[:0], errors.Wrap(err, "failed to retrieve container")
		}
		if rc.N() == 0 {
			return a[:0], nil
		}
		rc1, chng := rc.Remove(lo)
		if !chng {
			return a[:0], nil
		}
		if rc1.N() == 0 {
			err = tx.tx.RemoveContainer(name, hi)
		} else {
			err = tx.tx.PutContainer(name, hi, rc1)
		}
		if err != nil {
			return a[:0], err
		}
		return a[:1], nil
	}

	changeCount := 0
	changed = a

	var lastHi uint64 = math.MaxUint64 // highbits is always less than this starter.
	var rc *roaring.Container
	var hi uint64
	var lo uint16

	for i, v := range a {
		hi, lo = highbits(v), lowbits(v)
		if hi != lastHi {
			// either first time through, or changed to a different container.
			// do we need put the last updated container now?
			if i > 0 {
				// not first time through, write what we got.
				if rc == nil || rc.N() == 0 {
					err = tx.tx.RemoveContainer(name, lastHi)
					if err != nil {
						return a[:0], errors.Wrap(err, "failed to remove container")
					}
				} else {
					err = tx.tx.PutContainer(name, lastHi, rc)
					if err != nil {
						return a[:0], errors.Wrap(err, "failed to put container")
					}
				}
			}
			// get the next container
			rc, err = tx.tx.Container(name, hi)
			if err != nil {
				return a[:0], errors.Wrap(err, "failed to retrieve container")
			}
		} // else same container, keep adding bits to rct.
		chng := false
		rc, chng = rc.Remove(lo)
		if chng {
			changed[changeCount] = v
			changeCount++
		}
		lastHi = hi
	}
	// write the last updates.

	if rc == nil || rc.N() == 0 {
		err = tx.tx.RemoveContainer(name, hi)
		if err != nil {
			return a[:0], errors.Wrap(err, "failed to remove container")
		}
	} else {
		err = tx.tx.PutContainer(name, hi, rc)
		if err != nil {
			return a[:0], errors.Wrap(err, "failed to put container")
		}
	}
	return changed[:changeCount], nil
}

// sortedParanoia is a flag to enable a check for unsorted inputs to addOrRemove,
// which is expensive in practice and only really useful occasionally.
const sortedParanoia = false

func (tx *Tx) addOrRemove(key Key, remove bool, a ...uint64) (changeCount int, err error) {
	if len(a) == 0 {
		return 0, nil
	}
	name := key.Prefix()
	// this special case can/should possibly go away, except that it
	// turns out to be by far the most common case, and we need to know
	// there's at least two items to simplify the check-sorted thing.
	if len(a) == 1 {
		hi, lo := highbits(a[0]), lowbits(a[0])
		rc, err := tx.tx.Container(name, hi)
		if err != nil {
			return 0, errors.Wrap(err, "failed to retrieve container")
		}
		if remove {
			if rc.N() == 0 {
				return 0, nil
			}
			rc1, chng := rc.Remove(lo)
			if !chng {
				return 0, nil
			}
			if rc1.N() == 0 {
				err = tx.tx.RemoveContainer(name, hi)
			} else {
				err = tx.tx.PutContainer(name, hi, rc1)
			}
			if err != nil {
				return 0, err
			}
			return 1, nil
		} else {
			rc2, chng := rc.Add(lo)
			if !chng {
				return 0, nil
			}
			err = tx.tx.PutContainer(name, hi, rc2)
			if err != nil {
				return 0, err
			}
			return 1, nil
		}
	}

	var lastHi uint64 = math.MaxUint64 // highbits is always less than this starter.
	var rc *roaring.Container
	var hi uint64
	var lo uint16

	// we can accept sorted either ascending or descending.
	sign := a[1] - a[0]
	prev := a[0] - sign
	sign >>= 63
	for i, v := range a {
		// This check is noticably expensive (a few percent in some
		// use cases) and as long as it passes occasionally it's probably
		// not important to run it all the time, and anyway panic is
		// not a good choice outside of testing.
		if sortedParanoia {
			if (v-prev)>>63 != sign {
				explain := fmt.Sprintf("addOrRemove: %d < %d != %d < %d", v, prev, a[1], a[0])
				panic(explain)
			}
			if v == prev {
				explain := fmt.Sprintf("addOrRemove: %d twice", v)
				panic(explain)
			}
		}
		prev = v
		hi, lo = highbits(v), lowbits(v)
		if hi != lastHi {
			// either first time through, or changed to a different container.
			// do we need put the last updated container now?
			if i > 0 {
				// not first time through, write what we got.
				if remove && (rc == nil || rc.N() == 0) {
					err = tx.tx.RemoveContainer(name, lastHi)
					if err != nil {
						return 0, errors.Wrap(err, "failed to remove container")
					}
				} else {
					err = tx.tx.PutContainer(name, lastHi, rc)
					if err != nil {
						return 0, errors.Wrap(err, "failed to put container")
					}
				}
			}
			// get the next container
			rc, err = tx.tx.Container(name, hi)
			if err != nil {
				return 0, errors.Wrap(err, "failed to retrieve container")
			}
		} // else same container, keep adding bits to rct.
		chng := false
		// rc can be nil before, and nil after, in both Remove/Add below.
		// The roaring container add() and remove() methods handle this.
		if remove {
			rc, chng = rc.Remove(lo)
		} else {
			rc, chng = rc.Add(lo)
		}
		if chng {
			changeCount++
		}
		lastHi = hi
	}
	// write the last updates.
	if remove {
		if rc == nil || rc.N() == 0 {
			err = tx.tx.RemoveContainer(name, hi)
			if err != nil {
				return 0, errors.Wrap(err, "failed to remove container")
			}
		} else {
			err = tx.tx.PutContainer(name, hi, rc)
			if err != nil {
				return 0, errors.Wrap(err, "failed to put container")
			}
		}
	} else {
		if rc == nil || rc.N() == 0 {
			panic("there should be no way to have an empty bitmap AFTER an Add() operation")
		}
		err = tx.tx.PutContainer(name, hi, rc)
		if err != nil {
			return 0, errors.Wrap(err, "failed to put container")
		}
	}
	return
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }
