// Package vector implements version vectors: per-record causality tracking
// used to tell whether one write causally depends on another (safe to
// fast-forward) or whether two writes happened concurrently on different
// devices without either seeing the other's change (a real conflict).
package vector

// Vector maps a node id (device or server) to the number of writes that
// node has made to a given record. It is a value type; all operations
// return a new Vector rather than mutating in place.
type Vector map[string]uint64

// Relation describes how two version vectors relate to each other.
type Relation int

const (
	// Equal means both vectors have identical counters for every node.
	Equal Relation = iota
	// Ancestor means v happened-before other: other saw every write in v
	// plus at least one more.
	Ancestor
	// Descendant means v happened-after other: v saw every write in other
	// plus at least one more.
	Descendant
	// Concurrent means neither vector saw the other's writes: a genuine
	// conflict that requires deterministic resolution.
	Concurrent
)

// Clone returns an independent copy of v.
func (v Vector) Clone() Vector {
	out := make(Vector, len(v))
	for k, c := range v {
		out[k] = c
	}
	return out
}

// Increment returns a copy of v with node's counter incremented by one.
func (v Vector) Increment(node string) Vector {
	out := v.Clone()
	out[node] = out[node] + 1
	return out
}

// Compare determines the causal relationship of v to other.
func (v Vector) Compare(other Vector) Relation {
	vGreater, oGreater := false, false

	for node, c := range v {
		if oc := other[node]; c > oc {
			vGreater = true
		} else if c < oc {
			oGreater = true
		}
	}
	for node, oc := range other {
		if _, ok := v[node]; ok {
			continue // already compared above
		}
		if oc > 0 {
			oGreater = true
		}
	}

	switch {
	case !vGreater && !oGreater:
		return Equal
	case vGreater && !oGreater:
		return Descendant
	case !vGreater && oGreater:
		return Ancestor
	default:
		return Concurrent
	}
}

// Merge returns the component-wise maximum of v and other: a vector that
// dominates (or equals) both inputs. Used after resolving a conflict so
// that the merged record's vector reflects having seen both writes.
func Merge(a, b Vector) Vector {
	out := make(Vector, len(a)+len(b))
	for node, c := range a {
		out[node] = c
	}
	for node, c := range b {
		if c > out[node] {
			out[node] = c
		}
	}
	return out
}
