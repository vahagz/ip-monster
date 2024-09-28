package btree

import (
	"slices"
	"unsafe"

	"ip_addr_counter/pkg/array"
)

const (
	flagLeafNode byte = iota
	flagInternalNode
)

type Key interface {
	New() Key
	Copy() Key
	Size() int
	Compare(k2 Key) int
}

func KeyComparator[K Key](k1, k2 K) int {
	return k1.Compare(k2)
}

type nodeArray[L any] struct {
	data L
	len  int
}

type nodeData[KL, CL any] struct {
	isLeaf   bool
	count    int // count of keys currently inserted into node
	keys     nodeArray[KL]
	children nodeArray[CL]
}

// descriptor for nodeData
type node[I array.Integer, K Key, KL, CL any] struct {
	data     *nodeData[KL, CL]
	keys     []K
	children []I
}

func newNode[I array.Integer, K Key, KL, CL any](degree int, isLeaf bool) *node[I, K, KL, CL] {
	return newNodeWithData[I, K](&nodeData[KL, CL]{
		isLeaf:   isLeaf,
		keys:     nodeArray[KL]{len: 2 * degree - 1},
		children: nodeArray[CL]{len: 2 * degree},
	})
}

func newNodeWithData[I array.Integer, K Key, KL, CL any](data *nodeData[KL, CL]) *node[I, K, KL, CL] {
	keys := unsafe.Slice(
		(*K)(unsafe.Pointer(&data.keys.data)),
		data.keys.len,
	)
	children := unsafe.Slice(
		(*I)(unsafe.Pointer(&data.children.data)),
		data.children.len,
	)

	return &node[I, K, KL, CL]{
		data:     data,
		keys:     keys,
		children: children,
	}
}

func NodeSize[I array.Integer, K Key, KL, CL any]() int {
	var n node[I, K, KL, CL]
	return n.size()
}

func (n node[I, K, KL, CL]) size() int {
	return int(unsafe.Sizeof(*n.data))
}

func (n *node[I, K, KL, CL]) search(key K) (i int, found bool) {
	return slices.BinarySearchFunc(n.keys[:n.data.count], key, KeyComparator)
}
