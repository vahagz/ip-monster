package array

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"iter"

	"ip_addr_counter/pkg/file"
)

type array struct {
	file      file.Interface
	fileSize  uint64
	elemSize  uint64
	length    uint64
	offset    uint64
}

type Array interface {
	Get(index uint64) []byte
	GetCopy(index uint64) []byte
	Last() []byte
	Set(index uint64, val []byte)
	Push(val []byte) uint64
	Popn()
	Pop() []byte
	PopCopy() []byte
	Swap(i, j uint64)
	Len() uint64
	Cap() uint64
	Slice(from, to uint64) Array
	Truncate(size uint64)
	Grow(size uint64)
	File() file.Interface
	Iterator(cacheSize int) iter.Seq[[]byte]
}

func New(file file.Interface, elemSize, length uint64) Array {
	return &array{
		file:     file,
		fileSize: file.Size(),
		elemSize: elemSize,
		length:   length,
		offset:   0,
	}
}

func (a *array) Get(index uint64) []byte {
	a.checkBounds(index)
	index += a.offset
	return a.file.Slice(a.indexToOffset(index), uint64(a.elemSize))
}

func (a *array) GetCopy(index uint64) []byte {
	return bytes.Clone(a.Get(index))
}

func (a *array) Last() []byte {
	return a.Get(a.length - 1)
}

func (a *array) Set(index uint64, val []byte) {
	a.checkBounds(index)
	index += a.offset
	copy(a.file.Slice(a.indexToOffset(index), uint64(a.elemSize)), val)
}

func (a *array) Push(val []byte) uint64 {
	a.Grow(a.length + 1)
	a.Set(a.length - 1, val)
	return a.length - 1
}

func (a *array) Popn() {
	*a = *a.Slice(0, a.length - 1).(*array)
}

func (a *array) Pop() []byte {
	val := a.Get(a.length - 1)
	*a = *a.Slice(0, a.length - 1).(*array)
	return val
}

func (a *array) PopCopy() []byte {
	return bytes.Clone(a.Pop())
}

func (a *array) Swap(i, j uint64) {
	itm1, itm2 := a.GetCopy(i), a.GetCopy(j)
	a.Set(i, itm2)
	a.Set(j, itm1)
}

func (a *array) Len() uint64 {
	return a.length
}

func (a *array) Cap() uint64 {
	return a.fileSize / (a.elemSize - a.offset)
}

func (a *array) Slice(from, to uint64) Array {
	if from < 0 || from > to || to < 0 || to > a.Cap() {
		panic(fmt.Errorf("out of bounds: [%d:%d], len:%d, cap:%d", from, to, a.length, a.Cap()))
	}

	return &array{
		file:     a.file,
		fileSize: a.fileSize,
		elemSize: a.elemSize,
		length:   to - from,
		offset:   a.offset + from,
	}
}

func (a *array) Truncate(size uint64) {
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

func (a *array) Grow(size uint64) {
	if size <= a.length {
		return
	}

	if size > a.Cap() {
		a.Truncate(size)
	}
	a.length = size
}

func (a *array) File() file.Interface {
	return a.file
}

func (a *array) Iterator(cacheSize int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		elemSize := int(a.elemSize)
		elem := make([]byte, elemSize)
		bufferSize := elemSize * cacheSize
		file := bufio.NewReaderSize(a.file.Reader(), bufferSize)

		for range a.length {
			n, err := file.Read(elem)
			if n != elemSize || (err != nil && err != io.EOF) {
				panic(err)
			} else if !yield(elem) {
				break
			}
		}
	}
}

func (a *array) checkBounds(index uint64) {
	if index >= a.length {
		panic(fmt.Errorf("out of bounds: %d", index))
	}
}

func (a *array) indexToOffset(index uint64) uint64 {
	return index * a.elemSize
}
