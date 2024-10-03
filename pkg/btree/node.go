package btree

import (
	"slices"

	"ip_addr_counter/pkg/util"
)

const (
	flagLeafNode byte = iota
	flagInternalNode
)

type Key interface {
	util.Comparable
	New() Key
	Copy() Key
	Size() int
}

func KeyComparator[K Key](k1, k2 K) int {
	return k1.Compare(k2)
}

type node[K Key] struct {
	isLeaf   bool
	count    int // count of keys currently inserted into node
	keys     []K
	children []*node[K]
}

func newNode[K Key](degree int, isLeaf bool) *node[K] {
	n := &node[K]{
		isLeaf:   isLeaf,
		count:    0,
		keys:     make([]K, 2 * degree - 1),
	}
	if !isLeaf {
		n.children = make([]*node[K], 2 * degree)
	}
	return n
}

func (n *node[K]) search(key K) (i int, found bool) {
	return slices.BinarySearchFunc(n.keys[:n.count], key, KeyComparator)
}
