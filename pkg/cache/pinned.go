package cache

import "cmp"

type Pinned[T cmp.Ordered, C any] struct {
	Val   C
	key   T
	cache *Cache[T, C]
}

func (p *Pinned[T, C]) Unpin() {
	p.cache.Unpin(p.key)
}
