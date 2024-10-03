package btree

type Metadata[K Key] struct {
	Degree int
	Count  uint64
	Root   *node[K]
}
