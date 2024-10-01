package btree

import (
	"container/heap"
	a "ip_addr_counter/pkg/array"
)

type queueItem[K Key] struct {
	iterator <-chan K
	last     K
}

type iteratorQueue[K Key] []queueItem[K]

func (iq iteratorQueue[K]) Len() int {
	return len(iq)
}

func (iq iteratorQueue[K]) Less(i, j int) bool {
	return iq[i].last.Compare(iq[j].last) == -1
}

func (iq iteratorQueue[K]) Swap(i, j int) {
	iq[i], iq[j] = iq[j], iq[i]
}

func (iq *iteratorQueue[K]) Push(x any) {
	*iq = append(*iq, x.(queueItem[K]))
}

func (iq *iteratorQueue[K]) Pop() any {
	lastIndex := len(*iq) - 1
	top := (*iq)[lastIndex]
	*iq = (*iq)[:lastIndex]
	return top
}

func MultIterator[I a.Integer, K Key, KL, CL any](
	treeArr []*BTree[I, K, KL, CL],
	multiIteratorCacheSize, perTreeCacheSize int,
) <-chan K {
	ch := make(chan K, multiIteratorCacheSize)
	iq := &iteratorQueue[K]{}
	for _, t := range treeArr {
		iter := t.Iterator(perTreeCacheSize)
		last, ok := <-iter
		if ok {
			*iq = append(*iq, queueItem[K]{iterator: iter, last: last})
		}
	}

	heap.Init(iq)

	go func () {
		for iq.Len() > 0 {
			itm := heap.Pop(iq).(queueItem[K])
			last := itm.last
			next, ok := <-itm.iterator
			if ok {
				itm.last = next
				heap.Push(iq, itm)
			}
			ch <- last
		}
		close(ch)
	}()

	return ch
}
