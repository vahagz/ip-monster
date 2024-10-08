package array

import (
	"bufio"
	"fmt"
	"io"
	"iter"

	"ip_addr_counter/pkg/file"
)

type Array struct {
	file      file.Interface
	fileSize  uint64
	elemSize  uint64
	length    uint64
	offset    uint64
}

func New(file file.Interface, elemSize, length uint64) *Array {
	return &Array{
		file:     file,
		fileSize: file.Size(),
		elemSize: elemSize,
		length:   length,
		offset:   0,
	}
}

func (a *Array) Get(index uint64) []byte {
	a.checkBounds(index)
	index += a.offset
	return a.file.Slice(a.indexToOffset(index), uint64(a.elemSize))
}

func (a *Array) Set(index uint64, val []byte) {
	a.checkBounds(index)
	index += a.offset
	copy(a.file.Slice(a.indexToOffset(index), uint64(a.elemSize)), val)
}

func (a *Array) Push(val []byte) uint64 {
	a.Grow(a.length + 1)
	a.Set(a.length - 1, val)
	return a.length - 1
}

func (a *Array) Len() uint64 {
	return a.length
}

func (a *Array) Cap() uint64 {
	return a.fileSize / (a.elemSize - a.offset)
}

func (a *Array) Truncate(size uint64) {
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

func (a *Array) Grow(size uint64) {
	if size <= a.length {
		return
	}

	if size > a.Cap() {
		a.Truncate(size)
	}
	a.length = size
}

func (a *Array) File() file.Interface {
	return a.file
}

func (a *Array) FileReader() io.Reader {
	return a.file.LimitReader(int64(a.elemSize*a.length))
}

func (a *Array) Iterator(cacheSize int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		elemSize := int(a.elemSize)
		elem := make([]byte, elemSize)
		bufferSize := elemSize * cacheSize
		file := bufio.NewReaderSize(a.FileReader(), bufferSize)

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

func (a *Array) checkBounds(index uint64) {
	if index >= a.length {
		panic(fmt.Errorf("out of bounds: %d", index))
	}
}

func (a *Array) indexToOffset(index uint64) uint64 {
	return index * a.elemSize
}
