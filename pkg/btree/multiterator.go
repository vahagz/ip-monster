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

func (sq iteratorQueue[K]) Len() int {
	return len(sq)
}

func (sq iteratorQueue[K]) Less(i, j int) bool {
	return sq[i].last.Compare(sq[j].last) == -1
}

func (sq iteratorQueue[K]) Swap(i, j int) {
	sq[i], sq[j] = sq[j], sq[i]
}

func (sq *iteratorQueue[K]) Push(x any) {
	*sq = append(*sq, x.(queueItem[K]))
}

func (sq *iteratorQueue[K]) Pop() any {
	lastIndex := len(*sq) - 1
	top := (*sq)[lastIndex]
	*sq = (*sq)[:lastIndex]
	return top
}

func MultIterator[I a.Integer, K Key, KL, CL any](treeArr []*BTree[I, K, KL, CL]) <-chan K {
	ch := make(chan K, len(treeArr))
	sq := &iteratorQueue[K]{}
	for _, t := range treeArr {
		iter := t.Iterator()
		*sq = append(*sq, queueItem[K]{iterator: iter, last: <-iter})
	}

	heap.Init(sq)

	go func () {
		for sq.Len() > 0 {
			itm := heap.Pop(sq).(queueItem[K])
			ch <- itm.last
			next, ok := <-itm.iterator
			if ok {
				itm.last = next
				heap.Push(sq, itm)
			}
		}
		close(ch)
	}()

	return ch
}
