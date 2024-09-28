package array

import (
	"bytes"
	"container/heap"
	"ip_addr_counter/pkg/array"
)

type heapArray[T array.Integer] struct {
	arr array.Array[T]
}

func Heap[T array.Integer](arr array.Array[T]) heap.Interface {
	return &heapArray[T]{arr}
}

func (h *heapArray[T]) Push(x any) {
	h.arr.Push(x.([]byte))
}

func (h *heapArray[T]) Pop() any {
	return h.arr.PopCopy()
}

func (h *heapArray[T]) Len() int {
	return int(h.arr.Len())
}

func (h *heapArray[T]) Less(i, j int) bool {
	itm1, itm2 := h.arr.Get(T(i)), h.arr.Get(T(j))
	return bytes.Compare(itm2, itm1) == -1
}

func (h *heapArray[T]) Swap(i, j int) {
	h.arr.Swap(T(i), T(j))
}
