package rbtree

import (
	"ip_addr_counter/pkg/array"
)

type Metadata[T array.Integer] struct {
	NodeKeySize uint16
	Root        T
	Null        T
	Count       uint64
}
