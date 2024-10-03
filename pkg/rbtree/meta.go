package rbtree

type Integer interface {
	~int   | ~uint   |
	~uint8 | ~uint16 | ~uint32 | ~uint64 |
	~int8  | ~int16  | ~int32  | ~int64
}

type Metadata[T Integer] struct {
	NodeKeySize uint16
	Root        T
	Null        T
	Count       uint64
}
