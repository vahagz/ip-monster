package array

import (
	"context"
	"iter"
	"unsafe"

	"ip_addr_counter/pkg/array"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/util"
)

type Elem interface {  }

type ElemPointer[T any] interface {
	Elem
	*T
}

type Array[T Elem, PT ElemPointer[T]] interface {
	Get(index uint64) PT
	Push(val PT) uint64
	Len() uint64
	File() file.Interface
	Iterator(cacheSize int) iter.Seq[T]
}

type arrayGeneric[T Elem, PT ElemPointer[T]] struct {
	arr array.Array
}

func New[T Elem, PT ElemPointer[T]](file file.Interface, length uint64) Array[T, PT] {
	var t T
	return &arrayGeneric[T, PT]{
		arr: array.New(file, uint64(unsafe.Sizeof(t)), length),
	}
}

func (a *arrayGeneric[T, PT]) Get(index uint64) PT {
	return util.BytesTo[*T](a.arr.Get(index))
}

func (a *arrayGeneric[T, PT]) Set(index uint64, val PT) {
	*a.Get(index) = *val
}

func (a *arrayGeneric[T, PT]) Push(val PT) uint64 {
	a.arr.Grow(a.arr.Len() + 1)
	a.Set(a.arr.Len() - 1, val)
	return a.arr.Len() - 1
}

func (a *arrayGeneric[T, PT]) Len() uint64 {
	return a.arr.Len()
}

func (a *arrayGeneric[T, PT]) File() file.Interface {
	return a.arr.File()
}

func (a *arrayGeneric[T, PT]) Iterator(cacheSize int) iter.Seq[T] {
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
