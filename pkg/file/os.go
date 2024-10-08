package file

import (
	"io"
	"os"
)

type OSFile struct {
	file *os.File
}

func OS(f *os.File) *OSFile {
	return &OSFile{f}
}

func (of *OSFile) Truncate(size uint64) error {
	return of.file.Truncate(int64(size))
}

func (of *OSFile) Slice(from, n uint64) []byte {
	buf := make([]byte, n)
	of.file.ReadAt(buf, int64(from))
	return buf
}

func (of *OSFile) Size() uint64 {
	stat, err := of.file.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(stat.Size())
}

func (of *OSFile) LimitReader(n int64) io.Reader {
	of.file.Seek(0, io.SeekStart)
	return io.LimitReader(of.file, int64(n))
}
