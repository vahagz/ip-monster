package array

import (
	"bytes"
	"fmt"

	"ip_addr_counter/pkg/file"
)

type Integer interface {
	~int   | ~uint   |
	~uint8 | ~uint16 | ~uint32 | ~uint64 |
	~int8  | ~int16  | ~int32  | ~int64
}

type array[T Integer] struct {
	file      file.Interface
	fileSize  uint64
	elemSize  T
	length    T
	offset    T
}

type Array[T Integer] interface {
	Get(index T) []byte
	GetCopy(index T) []byte
	Last() []byte
	Set(index T, val []byte)
	Push(val []byte) T
	Popn()
	Pop() []byte
	PopCopy() []byte
	Swap(i, j T)
	Len() T
	Cap() T
	Slice(from, to T) Array[T]
	Truncate(size T)
	Grow(size T)
	File() file.Interface
	Iterator(cacheSize int) <-chan []byte
}

func New[T Integer](file file.Interface, elemSize int, length uint64) Array[T] {
	return &array[T]{
		file:     file,
		fileSize: file.Size(),
		elemSize: T(elemSize),
		length:   T(length),
		offset:   0,
	}
}

func (a *array[T]) Get(index T) []byte {
	a.checkBounds(index)
	index += a.offset
	return a.file.Slice(a.indexToOffset(index), uint64(a.elemSize))
}

func (a *array[T]) GetCopy(index T) []byte {
	return bytes.Clone(a.Get(index))
}

func (a *array[T]) Last() []byte {
	return a.Get(a.length - 1)
}

func (a *array[T]) Set(index T, val []byte) {
	a.checkBounds(index)
	index += a.offset
	copy(a.file.Slice(a.indexToOffset(index), uint64(a.elemSize)), val)
}

func (a *array[T]) Push(val []byte) T {
	a.Grow(a.length + 1)
	a.Set(a.length - 1, val)
	return a.length - 1
}

func (a *array[T]) Popn() {
	*a = *a.Slice(0, a.length - 1).(*array[T])
}

func (a *array[T]) Pop() []byte {
	val := a.Get(a.length - 1)
	*a = *a.Slice(0, a.length - 1).(*array[T])
	return val
}

func (a *array[T]) PopCopy() []byte {
	return bytes.Clone(a.Pop())
}

func (a *array[T]) Swap(i, j T) {
	itm1, itm2 := a.GetCopy(i), a.GetCopy(j)
	a.Set(i, itm2)
	a.Set(j, itm1)
}

func (a *array[T]) Len() T {
	return a.length
}

func (a *array[T]) Cap() T {
	return T(a.fileSize / uint64(a.elemSize - a.offset))
}

func (a *array[T]) Slice(from, to T) Array[T] {
	if from < 0 || from > to || to < 0 || to > a.Cap() {
		panic(fmt.Errorf("out of bounds: [%d:%d], len:%d, cap:%d", from, to, a.length, a.Cap()))
	}

	return &array[T]{
		file:     a.file,
		fileSize: a.fileSize,
		elemSize: a.elemSize,
		length:   to - from,
		offset:   a.offset + from,
	}
}

func (a *array[T]) Truncate(size T) {
	a.fileSize = uint64(size) * uint64(a.elemSize)
	err := a.file.Truncate(a.fileSize)
	if err != nil {
		panic(err)
	}

	cap := a.Cap()
	if a.length <= cap {
		a.length = cap
	}
}

func (a *array[T]) Grow(size T) {
	if size <= a.length {
		return
	}

	if size > a.Cap() {
		a.Truncate(size)
	}
	a.length = size
}

func (a *array[T]) File() file.Interface {
	return a.file
}

func (a *array[T]) Iterator(cacheSize int) <-chan []byte {
	ch := make(chan []byte, cacheSize)
	go func () {
		for i := T(0); i < a.length; i++ {
			ch <- a.Get(i)
		}
		close(ch)
	}()
	return ch
}

func (a *array[T]) checkBounds(index T) {
	if index >= a.length {
		panic(fmt.Errorf("out of bounds: %d", index))
	}
}

func (a *array[T]) indexToOffset(index T) uint64 {
	return uint64(index) * uint64(a.elemSize)
}
