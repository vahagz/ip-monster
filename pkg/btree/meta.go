package btree

import "ip_addr_counter/pkg/array"

type Metadata[T array.Integer] struct {
	Degree int
	Count  uint64
	Root   T
}
