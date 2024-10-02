package array

import (
	"bytes"
	"container/heap"
	"fmt"
	"time"

	"ip_addr_counter/pkg/array"
)

func HeapSort[T array.Integer](arr array.Array[T]) array.Array[T] {
	initialArr := arr
	lastIndex := initialArr.Len() - 1

	fmt.Println("heap.init")
	initStart := time.Now()
	heap.Init(Heap(arr))
	fmt.Printf(
		"heap.init duration %ds\n",
		uint64(time.Since(initStart).Seconds()),
	)

	arr.Swap(0, arr.Len() - 1)
	arr = arr.Slice(0, arr.Len() - 1)

	start := time.Now()
	stop := false
	defer func() {
		stop = true
	}()
	go func() {
		for !stop {
			time.Sleep(time.Second)
			fmt.Printf(
				"%ds, %v sorted elements per second\n",
				uint64(time.Since(start).Seconds()), int64(initialArr.Len() - arr.Len()) / int64(time.Since(start).Seconds()),
			)
		}
	}()

	for arr.Len() > 0 {
		heap.Fix(Heap(arr), 0)
		root := arr.GetCopy(0)
		last := arr.Last()
		arr.Set(0, last)
		if bytes.Compare(initialArr.Get(lastIndex), root) != 0 {
			lastIndex--
			initialArr.Set(lastIndex, root)
		}

		arr = arr.Slice(0, arr.Len() - 1)
	}

	return initialArr.Slice(lastIndex, initialArr.Len())
}
