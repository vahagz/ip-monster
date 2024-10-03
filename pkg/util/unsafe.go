package util

import "unsafe"

type pointer[T any] interface {
	*T
}

func ToBytes[T any, P pointer[T]](ptr P) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(ptr)), unsafe.Sizeof(*ptr))
}

func BytesTo[P pointer[T], T any](buf []byte) P {
	return (P)(unsafe.Pointer(&buf[0]))
}

func BytesToString(bytes []byte) string {
	return unsafe.String(unsafe.SliceData(bytes), len(bytes))
}
