package rbtree

import (
	"ip_addr_counter/pkg/array"
	"unsafe"
)

type Metadata[T array.Integer] struct {
	nodeKeySize uint16
	root        T
	null        T
	count       uint64
}

func (m Metadata[T]) size() int {
	return int(unsafe.Sizeof(m))
}

func (m *Metadata[T]) MarshalBinary() ([]byte, error) {
	sz := m.size()
	buf := make([]byte, sz)
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(&m)), sz))
	return buf, nil
}

func (m *Metadata[T]) UnmarshalBinary(d []byte) error {
	*m = *(*Metadata[T])(unsafe.Pointer(&d[0]))
	return nil
}
