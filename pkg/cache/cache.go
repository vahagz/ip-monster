package cache

import (
	"cmp"
	"fmt"

	"github.com/emirpasic/gods/maps/treemap"
)

type item[T any, C any] struct {
	priority uint64
	key      T
	val      C
}

type Cache[T cmp.Ordered, C any] struct {
	size     int
	items    *treemap.Map
	priority *treemap.Map
	pinned   map[T]C
	onDelete func(key T, item C)
}

func New[T cmp.Ordered, C any](size int, onDelete func(key T, item C)) *Cache[T, C] {
	return &Cache[T, C]{
		size:     size,
		onDelete: onDelete,

		items: treemap.NewWith(func(a, b interface{}) int {
			return cmp.Compare(a.(T), b.(T))
		}),
		priority: treemap.NewWith(func(a, b interface{}) int {
			ai, bi := a.(item[T, C]), b.(item[T, C])
			res := cmp.Compare(ai.priority, bi.priority)
			if res == 0 {
				return cmp.Compare(ai.key, bi.key)
			}
			return res
		}),
		pinned: map[T]C{},
	}
}

func (c *Cache[T, C]) Add(key T, val C) {
	if _, ok := c.pinned[key]; ok {
		panic(fmt.Errorf("can't add already pinned key: %v", key))
	} else if _, ok := c.items.Get(key); ok {
		return
	}

	if c.priority.Size() >= c.size {
		keyToDelete, _ := c.priority.Min()
		c.freeSpace(keyToDelete.(item[T, C]))
	}

	itm := item[T, C]{val: val, key: key, priority: 1}
	c.priority.Put(itm, struct{}{})
	c.items.Put(key, itm)
}

func (c *Cache[T, C]) Get(key T) (C, bool) {
	if val, ok := c.pinned[key]; ok {
		return val, true
	}

	val, ok := c.items.Get(key)
	if !ok {
		var c C
		return c, false
	}

	itm := val.(item[T, C])
	c.priority.Remove(itm)
	itm.priority++
	c.priority.Put(itm, struct{}{})
	c.items.Put(itm.key, itm)
	return itm.val, true
}

func (c *Cache[T, C]) Del(key T) {
	if _, ok := c.pinned[key]; ok {
		return
	}

	val, ok := c.items.Get(key)
	if !ok {
		return
	}

	c.priority.Remove(val.(item[T, C]))
	c.items.Remove(key)
}

func (c *Cache[T, C]) Clear() {
	c.items.Clear()
	c.priority.Clear()
}

func (c *Cache[T, C]) Flush() {
	for _, pr := range c.priority.Keys() {
		c.freeSpace(pr.(item[T, C]))
	}
	c.Clear()
}

func (c *Cache[T, C]) Pin(key T, val C) *Pinned[T, C] {
	c.Del(key)
	c.pinned[key] = val
	return &Pinned[T, C]{
		Val:   val,
		key:   key,
		cache: c,
	}
}

// func (c *Cache[T, C]) Pin(key T, val C) {
// 	c.Del(key)
// 	c.pinned[key] = val
// }

func (c *Cache[T, C]) Unpin(key T) {
	val, ok := c.pinned[key]
	if !ok {
		return
	}

	delete(c.pinned, key)
	c.Add(key, val)
}

func (c *Cache[T, C]) freeSpace(itm item[T, C]) {
	c.priority.Remove(itm)
	c.onDelete(itm.key, itm.val)
	c.items.Remove(itm.key)
}
