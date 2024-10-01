package util

import (
	"container/heap"
)

type Iterable[T any] interface {
	Iterator(bufferSize int) <-chan T
}

type Comparable interface {
	Compare(t Comparable) int
}

type queueItem[T Comparable] struct {
	iterator <-chan T
	last     T
}

type iteratorQueue[T Comparable] []queueItem[T]

func (iq iteratorQueue[T]) Len() int {
	return len(iq)
}

func (iq iteratorQueue[T]) Less(i, j int) bool {
	return iq[i].last.Compare(iq[j].last) == -1
}

func (iq iteratorQueue[T]) Swap(i, j int) {
	iq[i], iq[j] = iq[j], iq[i]
}

func (iq *iteratorQueue[T]) Push(x any) {
	*iq = append(*iq, x.(queueItem[T]))
}

func (iq *iteratorQueue[T]) Pop() any {
	lastIndex := len(*iq) - 1
	top := (*iq)[lastIndex]
	*iq = (*iq)[:lastIndex]
	return top
}

func MultIterator[T Comparable](
	iteratorArr []Iterable[T],
	multiIteratorCacheSize, perIteratorCacheSize int,
) <-chan T {
	ch := make(chan T, multiIteratorCacheSize)
	iq := &iteratorQueue[T]{}
	for _, it := range iteratorArr {
		iter := it.Iterator(perIteratorCacheSize)
		last, ok := <-iter
		if ok {
			*iq = append(*iq, queueItem[T]{iterator: iter, last: last})
		}
	}

	heap.Init(iq)

	go func () {
		for iq.Len() > 0 {
			itm := heap.Pop(iq).(queueItem[T])
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
