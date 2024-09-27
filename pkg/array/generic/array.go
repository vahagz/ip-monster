package array

import (
	"encoding"
	"unsafe"

	"ip_addr_counter/pkg/array"
	"ip_addr_counter/pkg/file"
)

type Elem interface {
	encoding.BinaryMarshaler
}

type ElemPointer[T any] interface {
	Elem
	encoding.BinaryUnmarshaler
	*T
}

type Array[I array.Integer, T Elem, PT ElemPointer[T]] interface {
	Get(index I) PT
	Last() PT
	Set(index I, val T)
	Push(val T) I
	Popn()
	Pop() PT
	Swap(i, j I)
	Len() I
	Cap() I
	Slice(from, to I) Array[I, T, PT]
}

type arrayGeneric[I array.Integer, T Elem, PT ElemPointer[T]] struct {
	arr array.Array[I]
}

func New[I array.Integer, T Elem, PT ElemPointer[T]](
	file file.Interface,
	elemSize int,
	length uint64,
) Array[I, T, PT] {
	return &arrayGeneric[I, T, PT]{
		arr: array.New[I](file, elemSize, length),
	}
}

func (a *arrayGeneric[I, T, PT]) Get(index I) PT {
	return (*T)(unsafe.Pointer(&a.arr.Get(index)[0]))
}

func (a *arrayGeneric[I, T, PT]) Last() PT {
	return a.Get(a.Len() - 1)
}

func (a *arrayGeneric[I, T, PT]) Set(index I, val T) {
	*a.Get(index) = val
}

func (a *arrayGeneric[I, T, PT]) Push(val T) I {
	a.arr.Grow(a.arr.Len() + 1)
	a.Set(a.arr.Len() - 1, val)
	return a.arr.Len() - 1
}

func (a *arrayGeneric[I, T, PT]) Popn() {
	index := a.Len() - 1
	*a = *a.Slice(0, index).(*arrayGeneric[I, T, PT])
}

func (a *arrayGeneric[I, T, PT]) Pop() PT {
	index := a.Len() - 1
	elem := a.Get(index)
	*a = *a.Slice(0, index).(*arrayGeneric[I, T, PT])
	return elem
}

func (a *arrayGeneric[I, T, PT]) Swap(i, j I) {
	a.arr.Swap(i, j)
}

func (a *arrayGeneric[I, T, PT]) Len() I {
	return a.arr.Len()
}

func (a *arrayGeneric[I, T, PT]) Cap() I {
	return a.arr.Cap()
}

func (a *arrayGeneric[I, T, PT]) Slice(from, to I) Array[I, T, PT] {
	return &arrayGeneric[I, T, PT]{
		arr: a.arr.Slice(from, to),
	}
}

func (a *arrayGeneric[I, T, PT]) Truncate(size I) {
	a.arr.Truncate(size)
}
