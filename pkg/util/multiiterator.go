package util

import (
	"container/heap"
	"iter"
)

type Comparable interface {
	Compare(t Comparable) int
}

type queueItem[T Comparable] struct {
	next func() (T, bool)
	stop func()
	last T
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

func MultiIterator[T Comparable](iteratorArr []iter.Seq[T]) iter.Seq[T] {
	iq := &iteratorQueue[T]{}
	for _, it := range iteratorArr {
		next, stop := iter.Pull(it)
		last, ok := next()
		if ok {
			*iq = append(*iq, queueItem[T]{next: next, stop: stop, last: last})
		}
	}

	heap.Init(iq)

	return func(yield func(T) bool) {
		for iq.Len() > 0 {
			itm := heap.Pop(iq).(queueItem[T])
			last := itm.last
			next, ok := itm.next()
			if ok {
				itm.last = next
				heap.Push(iq, itm)
			}

			if !yield(last) {
				itm.stop()
				break
			}
		}
	}
}
