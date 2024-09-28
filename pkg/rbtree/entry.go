package rbtree

type Key interface {
	New() Key
	Copy() Key
	Size() int
	Compare(k2 Key) int
}

func KeySize[K Key]() int {
	var k K
	return k.Size()
}
