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

func SetInterval(f func(start, now time.Time), interval time.Duration) {
	start := time.Now()
	go func ()  {
		for range time.Tick(time.Second) { f(start, time.Now()) }
	}()
}
