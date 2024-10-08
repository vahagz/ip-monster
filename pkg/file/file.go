package file

import "io"

type Interface interface {
	Truncate(size uint64) error
	Slice(from, n uint64) []byte
	Size() uint64
	Reader() io.Reader
}
