package components

import (
	"fmt"
	"iter"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/util"
)

func Write(cfg *WrtieConfigs) [][]*Array {
	// opening file with raw ip addresses
	ipFile := util.Must(os.OpenFile(cfg.IPFilePath, os.O_RDONLY, os.ModePerm))

	// breaking file into equal size segments (ipIteratorCount), for parallel reading
	ipIterators := ip.Iterator(ipFile, cfg.IPReaderPageSize, cfg.IPReaderCacheSize, cfg.IPIteratorCount)

	// slice of on-disk arrays. Each []Array is list of on-disk arrays stored
	// in files and read from single segment
	arrListPerStage := make([][]*Array, cfg.IPIteratorCount)

	// count of ips read from ip file and written into btrees
	writeCount := uint64(0)

	elementsPerStage := uint64(cfg.ElementsPerStage)
	arrVirtualFileSize := elementsPerStage * uint64(ipSize)

	// printing progress each second
	stop := util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"writeCount %d, sec %d, eps %d\n",
			writeCount, uint64(sec), writeCount / uint64(sec),
		)
	}, time.Second)
	defer stop()

	wg := &sync.WaitGroup{}
	for i, ipIterator := range ipIterators {
		wg.Add(1)
		// reading each segment in separate goroutine
		go func (i int, ipIterator iter.Seq[uint32]) {
			defer wg.Done()
			var stageWG *sync.WaitGroup
			stage := 0

			// initializing current btree
			current := btree.New[IP](cfg.BTDegree)

			// prepare helper function which will move filled in-memory btree
			// into on-disk sorted array
			processStage := stageProcessor(cfg.DstPath, cfg.Prefix, i, arrVirtualFileSize, &arrListPerStage[i])

			for ip := range ipIterator {
				atomic.AddUint64(&writeCount, 1)
				current.Put(IP(ip))

				// checking if btree is filled enough to store in on-disk array
				if current.Count() == elementsPerStage {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount)
					// flushing btree data into on-disk array and creating new one
					stageWG = processStage(current)
					current = btree.New[IP](cfg.BTDegree)
					stage++
				}
			}

			// checking if processStage was executed at least once
			if stageWG != nil {
				// wait if previous stage processing didn't finished 
				stageWG.Wait()
			}

			// check if segment wasn't completely read and some in-memory data left
			if current.Count() != elementsPerStage && current.Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount)
				// process rest data
				processStage(current).Wait()
			}
		}(i, ipIterator)
	}

	wg.Wait() // waiting for ip file to be completely read
	return arrListPerStage
}
