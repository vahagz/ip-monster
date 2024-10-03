package file

import (
	"os"
	"sync"
)

type OSFile struct {
	pools     map[uint64]*sync.Pool
	file      *os.File
	pageSize  int
	pageIndex int
	page      []byte
}

func NewFromOSFile(f *os.File, pageSize int) Interface {
	return &OSFile{
		pools:     map[uint64]*sync.Pool{},
		file:      f,
		pageSize:  pageSize,
		pageIndex: -1,
		page:      make([]byte, pageSize),
	}
}

func (of *OSFile) Truncate(size uint64) error {
	of.pageIndex = -1
	return of.file.Truncate(int64(size))
}

func (of *OSFile) Slice(from, n uint64) []byte {
	pageIndex := from / uint64(of.pageSize)
	off := int64(of.pageIndex * of.pageSize)

	if of.pageIndex == -1 || uint64(of.pageIndex) != pageIndex {
		of.pageIndex = int(pageIndex)
		off = int64(of.pageIndex * of.pageSize)
		of.file.ReadAt(of.page, off)
	}

	relativeOffset := from - uint64(off)
	return of.page[relativeOffset:relativeOffset + n]
}

func (of *OSFile) Size() uint64 {
	stat, err := of.file.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(stat.Size())
}

func (of *OSFile) Return(buf []byte) {
	// of.pools[uint64(len(buf))].Put(buf)
}

func (of *OSFile) getBuf(n uint64) []byte {
	p, ok := of.pools[n]
	if !ok {
		p = &sync.Pool{
			New: func() any {
				return make([]byte, n)
			},
		}
		of.pools[n] = p
	}

	return p.Get().([]byte)
}
