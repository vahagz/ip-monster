package file

import (
	"os"
)

type OSFile struct {
	file *os.File
}

func NewFromOSFile(f *os.File) Interface {
	return &OSFile{f}
}

func (of *OSFile) Truncate(size uint64) error {
	return of.file.Truncate(int64(size))
}

func (of *OSFile) Slice(from, n uint64) []byte {
	b := make([]byte, n)
	of.file.ReadAt(b, int64(from))
	return b
}

func (of *OSFile) Size() uint64 {
	stat, err := of.file.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(stat.Size())
}

func (of *OSFile) WriteTo(f *os.File) (n int64, err error) {
	return f.ReadFrom(of.file)
}

// func (of *OSFile) ReadAt(b []byte, off int64) (n int, err error) {
// 	return of.file.ReadAt(b, off)
// }

// func (of *OSFile) WriteAt(b []byte, off int64) (n int, err error) {
// 	return of.file.WriteAt(b, off)
// }
