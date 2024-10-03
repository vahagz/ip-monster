package util

import (
	"sync"
)

func CloseChan[T any](ch chan T) {
	defer func() { recover() }()
	close(ch)
}

func NewChanManager[T any](ch chan T) *ChanManager[T] {
	return &ChanManager[T]{
		ch:     ch,
		mutex:  &sync.Mutex{},
		closed: false,
	}
}

type ChanManager[T any] struct {
	ch     chan T
	mutex  *sync.Mutex
	closed bool
}

func (c *ChanManager[T]) Send(val T) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.closed {
		c.ch<-val
	}
}

func (c *ChanManager[T]) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.closed {
		CloseChan(c.ch)
		c.closed = true
	}
}
