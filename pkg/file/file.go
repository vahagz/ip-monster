package file

import (
	"bytes"
	"fmt"
	"os"
)

func New() *VirtualFile {
	return &VirtualFile{}
}

type Interface interface {
	Truncate(size uint64) error
	Slice(from, n uint64) []byte
	Size() uint64
}

type VirtualFile struct {
	data []byte
}

func (vf *VirtualFile) Size() uint64 {
	return uint64(len(vf.data))
}

func (vf *VirtualFile) Truncate(size uint64) error {
	if size < 0 {
		return fmt.Errorf("negative truncate size")
	} else if size <= uint64(cap(vf.data)) {
		vf.data = vf.data[:size]
	}
	data := make([]byte, size)
	copy(data, vf.data)
	vf.data = data
	return nil
}

func (vf *VirtualFile) Slice(from, n uint64) []byte {
	return vf.data[from:from+n]
}

// func (vf *VirtualFile) ReadAt(b []byte, off int64) (n int, err error) {
// 	if off >= int64(len(vf.data)) {
// 		return 0, io.EOF
// 	}
// 	return copy(b, vf.data[off:]), nil
// }

// func (vf *VirtualFile) WriteAt(b []byte, off int64) (n int, err error) {
// 	if off >= int64(len(vf.data)) {
// 		return 0, io.EOF
// 	}
// 	n = copy(vf.data[off:], b)
// 	if n != len(b) {
// 		err = io.EOF
// 	}
// 	return n, err
// }

func (vf *VirtualFile) WriteTo(f *os.File) (n int64, err error) {
	return f.ReadFrom(bytes.NewBuffer(vf.data))
}
