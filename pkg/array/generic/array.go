package array

import (
	"context"
	"io"
	"iter"
	"unsafe"

	"ip_addr_counter/pkg/array"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/util"
)

type Array[T any] struct {
	arr *array.Array
}

func New[T any](file file.Interface, length uint64) *Array[T] {
	var t T
	return &Array[T]{
		arr: array.New(file, uint64(unsafe.Sizeof(t)), length),
	}
}

func (a *Array[T]) Get(index uint64) *T {
	return util.BytesTo[*T](a.arr.Get(index))
}

func (a *Array[T]) Set(index uint64, val *T) {
	*a.Get(index) = *val
}

func (a *Array[T]) Push(val *T) uint64 {
	a.arr.Grow(a.arr.Len() + 1)
	a.Set(a.arr.Len() - 1, val)
	return a.arr.Len() - 1
}

func (a *Array[T]) Len() uint64 {
	return a.arr.Len()
}

func (a *Array[T]) File() file.Interface {
	return a.arr.File()
}

func (a *Array[T]) FileReader() io.Reader {
	return a.arr.FileReader()
}

func (a *Array[T]) Iterator(cacheSize int) iter.Seq[T] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan T, cacheSize)
	go func() {
		L: for itm := range a.arr.Iterator(cacheSize) {
			select {
			case <-ctx.Done():
				break L
			case ch <- *util.BytesTo[*T](itm):
				break
			}
		}
		close(ch)
	}()

	return func(yield func(T) bool) {
		for itm := range ch {
			if !yield(itm) {
				cancel()
				break
			}
		}
	}
}
