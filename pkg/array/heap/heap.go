package array

import (
	"bytes"
	"container/heap"

	"ip_addr_counter/pkg/array"
)

type heapArray struct {
	arr array.Array
}

func Heap(arr array.Array) heap.Interface {
	return &heapArray{arr}
}

func (h *heapArray) Push(x any) {
	h.arr.Push(x.([]byte))
}

func (h *heapArray) Pop() any {
	return h.arr.PopCopy()
}

func (h *heapArray) Len() int {
	return int(h.arr.Len())
}

func (h *heapArray) Less(i, j int) bool {
	itm1, itm2 := h.arr.Get(uint64(i)), h.arr.Get(uint64(j))
	return bytes.Compare(itm2, itm1) == -1
}

func (h *heapArray) Swap(i, j int) {
	h.arr.Swap(uint64(i), uint64(j))
}
