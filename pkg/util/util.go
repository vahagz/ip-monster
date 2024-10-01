package util

import (
	"time"
)

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Must[T any](val T, err error) T {
	PanicIfErr(err)
	return val
}

func SetInterval(f func(start, now time.Time), interval time.Duration) (stop func()) {
	start := time.Now()
	stopChan := make(chan struct{}, 1)
	go func () {
		L: for {
			select {
			case now := <-time.Tick(time.Second):
				f(start, now)
			case <-stopChan:
				close(stopChan)
				break L
			}
		}
	}()

	return func() {
		stopChan <- struct{}{}
	}
}
