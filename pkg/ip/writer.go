package ip

import (
	"bufio"
	"os"
)

type writer struct {
	dst      *os.File
	pageSize int
	buf      *bufio.Writer
}

func NewWriter(dst *os.File, pageSize int) *writer {
	return &writer{
		dst:      dst,
		buf:      bufio.NewWriterSize(dst, pageSize),
		pageSize: pageSize,
	}
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.buf.Write(p)
}

func (w *writer) Flush() error {
	return w.buf.Flush()
}
