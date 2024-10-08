package components

import (
	"fmt"
	"iter"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"ip_addr_counter/pkg/util"
)

func Read(cfg *ReadConfigs) uint64 {
	// count of read ip addresses from array files
	readCountPerSegment := make([]uint64, len(cfg.ArrayListPerStage))

	// count of unique ip addresses
	uniqCount := uint64(0)
	uniqCountPerSegment := make([]uint64, len(cfg.ArrayListPerStage))

	// printing progress each second
	stop := util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		readCount := uint64(0)
		uniqCount := uint64(0)
		for i := range len(cfg.ArrayListPerStage) {
			readCount += readCountPerSegment[i]
			uniqCount += uniqCountPerSegment[i]
		}

		fmt.Printf(
			"readCount %d, uniqCount %d, sec %d, eps %d\n",
			readCount, uniqCount, uint64(sec), readCount / uint64(sec),
		)
	}, time.Second)
	defer stop()

	// this two nested cycles are needed to distribute load on disk.
	// Actually just limits simultaneously running goroutines to parallelArrayReaderCount
	// It creates no more than parallelArrayReaderCount goroutines each of which
	// reads array lists created by index'th segment
	for i := range int(math.Ceil(float64(len(cfg.ArrayListPerStage)) / float64(cfg.ParallelArrayReaderCount))) {
		wg := &sync.WaitGroup{}

		for j := range cfg.ParallelArrayReaderCount {
			index := i * cfg.ParallelArrayReaderCount + j
			if index == len(cfg.ArrayListPerStage) {
				break
			}

			last := IP(math.MaxUint32)
			arrList := cfg.ArrayListPerStage[index]
			iterators := make([]iter.Seq[IP], len(arrList))
			for i := range arrList {
				iterators[i] = arrList[i].Iterator(cfg.ArrayIteratorCacheSize)
			}

			wg.Add(1)
			go func () {
				defer wg.Done()
				// reading values from list of iterators by increasing order
				for ip := range util.MultiIterator(iterators) {
					readCountPerSegment[index]++
					// since ips are being read in increasing order
					// uniqCount must be incremented only when previous ip
					// is not equal to current ip
					if last != ip {
						last = ip
						uniqCountPerSegment[index]++
					}
				}

				atomic.AddUint64(&uniqCount, uniqCountPerSegment[index])
			}()
		}

		wg.Wait()
	}

	return uniqCount
}