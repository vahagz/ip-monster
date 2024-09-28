package rbtree

import (
	"ip_addr_counter/pkg/array"
	"unsafe"
)

func newNode[I array.Integer, K Key](key K) *node[I, K] {
	return &node[I, K]{
		flags: FV_COLOR_RED,
		key:   key.Copy().(K),
	}
}

func emptyNode[I array.Integer, K Key]() *node[I, K] {
	var k K
	return &node[I, K]{
		key: k.New().(K),
	}
}

type flagVaue byte

const (
	FV_COLOR_BLACK flagVaue = 0b00000000
	FV_COLOR_RED   flagVaue = 0b00000001
)

type flagType byte

const (
	FT_COLOR flagType = 0
)

type node[I array.Integer, K Key] struct {
	left   I
	right  I
	parent I
	flags  flagVaue
	key    K
}

func NodeSize[I array.Integer, K Key]() int {
	var n node[I, K]
	return n.size()
}

func (n node[I, K]) size() int {
	return int(unsafe.Sizeof(n))
}

func (n *node[I, K]) isBlack() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_BLACK
}

func (n *node[I, K]) isRed() bool {
	return n.getFlag(FT_COLOR) == FV_COLOR_RED
}

func (n *node[I, K]) setBlack() {
	n.setFlag(FT_COLOR, FV_COLOR_BLACK)
}

func (n *node[I, K]) setRed() {
	n.setFlag(FT_COLOR, FV_COLOR_RED)
}

func (n *node[I, K]) setFlag(ft flagType, fv flagVaue) {
	mask := ^(byte(1) << ft)
	mask &= byte(n.flags)
	n.flags = flagVaue(mask) | fv
}

func (n *node[I, K]) getFlag(ft flagType) flagVaue {
	return n.flags & flagVaue(byte(1)<<byte(ft))
}

// func (n node[I, K]) MarshalBinary() ([]byte, error) {
// 	sz := n.size()
// 	buf := make([]byte, sz)
// 	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(&n)), sz))
// 	return buf, nil
// }

// func (n *node[I, K]) UnmarshalBinary(d []byte) error {
// 	*n = *(*node[I, K])(unsafe.Pointer(&d[0]))
// 	return nil
// }
