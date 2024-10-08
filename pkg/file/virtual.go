package file

import (
	"bytes"
	"fmt"
	"io"
)

func New() *VirtualFile {
	return &VirtualFile{}
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

func (vf *VirtualFile) Reader() io.Reader {
	return bytes.NewReader(vf.data)
}
