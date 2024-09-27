package rbtree

type EntryItem interface {
	New() EntryItem
	Copy() EntryItem
	Size() int
	IsNil() bool
	Compare(k2 EntryItem) int
}
