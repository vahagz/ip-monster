package stack

import (
	"errors"
)

var ErrEmptyStack = errors.New("empty stack")

type stack[T interface{}] struct {
	s []T
}

type Stack[T interface{}] interface {
	Push(v T)
	Pop() T
	Top() T
	Size() int
}

func New[T interface{}](initialSize int) Stack[T] {
	return &stack[T]{make([]T, 0, initialSize)}
}

func (s *stack[T]) Push(value T) {
	s.s = append(s.s, value)
}

func (s *stack[T]) Pop() T {
	l := len(s.s)
	if l == 0 {
		panic(ErrEmptyStack)
	}

	value := s.s[l-1]
	s.s = s.s[:l-1]
	return value
}

func (s *stack[T]) Top() T {
	l := len(s.s)
	if l == 0 {
		panic(ErrEmptyStack)
	}

	return s.s[l-1]
}

func (s *stack[T]) Size() int {
	return len(s.s)
}
