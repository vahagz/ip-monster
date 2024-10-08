package main

import (
	"fmt"
	"iter"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/util"
)

// folder where file with ip addresses. is located
const dataFolder = "data"

// name of the file with ips.
const ipFile = "ip_addresses.txt"

// folder where intermediate files will be placed.
const dstFolder = "dst";

// prefix for intermediate files created while counting.
const prefix = "array"

// parallel ip readers count. Each reader processes its own array files.
// readers are distributed linearly between ipFile.
const ipIteratorCount = 20

// count of elements to read for each iterator before processing to next stage.
const elementsToRead = 10_000_000

// min amount of data for read while reading ipFile.
const ipReaderPageSize = 4 * 1024 * 1024 // 4MB

// max count of ip addresses to store in memory while reading ipFile.
const ipReaderCacheSize = 1024

// degree of intermediate btrees.
// More degree - more memory saving but slower insertion.
const btreeDegree = 20

// count of goroutines reading final array files.
// Must be less or equal to ipIteratorCount
const parallelArrayReaderCount = 20

// count of ips for single read operation when iterating through array
const arrayIteratorCacheSize = 1024 * 1024

// min count of bytes required to store ipv4.
// used to specify btree key and array element size.
const ipSize = int(unsafe.Sizeof(uint32(0)))

// size of in-memory array file
const arrayVirtualFileSize = elementsToRead * ipSize

type BTree = btree.BTree[IP]

type Array = array.Array[IP]

// btree key (aka ip). Implements btree.Key interface
type IP uint32

func (k IP) Compare(k2 util.Comparable) int {
	k2Casted := k2.(IP)
	if k < k2Casted {
		return -1
	} else if k > k2Casted {
		return 1
	}
	return 0
}

// returns helpers function for converting btree into array
func stageProcessor(
	pwd string,
	i int,
	arrList *[]*Array,
) func(t *BTree) *sync.WaitGroup {
	m := &sync.Mutex{}
	arrayVFPool := &sync.Pool{New: func() any {
		vf := file.Virtual()
		vf.Truncate(uint64(arrayVirtualFileSize))
		return vf
	}}

	return func(t *BTree) *sync.WaitGroup {
		// wait if previous call didn't finished yet
		m.Lock()

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func () {
			defer wg.Done()
			defer m.Unlock()

			// initializing in-memory array to copy btree keys in increasing order
			arr := array.New[IP](arrayVFPool.Get().(*file.VirtualFile), 0)

			// creating file for array
			f := util.Must(os.OpenFile(
				path.Join(pwd, dataFolder, dstFolder, fmt.Sprintf("%s_%d_%d", prefix, i, len(*arrList))),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				os.ModePerm,
			))

			// scanning btree and pushing to array
			for k := range t.Iterator() {
				arr.Push(&k)
			}

			// copying array in-memory data to file
			f.ReadFrom(arr.FileReader())

			// returning array virtual file to pool for reuse
			arrayVFPool.Put(arr.File().(*file.VirtualFile))

			util.PanicIfErr(f.Sync())
			*arrList = append(*arrList, array.New[IP](
				file.OS(f),
				t.Count(),
			))
		}()

		return wg
	}
}

func main() {
	fmt.Println("============ WRITING PHASE ============")
	start := time.Now()
	pwd := util.Must(os.Getwd())

	// opening file with raw ip addresses
	ipFilePath := path.Join(pwd, dataFolder, ipFile)
	ipFile := util.Must(os.OpenFile(ipFilePath, os.O_RDONLY, os.ModePerm))

	// breaking file into equal size segments (ipIteratorCount), for parallel reading
	ipIterators := ip.Iterator(ipFile, ipReaderPageSize, ipReaderCacheSize, ipIteratorCount)

	// slice of on-disk arrays. Each []Array is list of on-disk arrays stored
	// in files and read from single segment
	arrListPerStage := make([][]*Array, ipIteratorCount)

	// list of trees currently being filled with ips
	current := make([]*BTree, ipIteratorCount)

	// count of ips read from ip file and written into btrees
	writeCount := uint64(0)

	// printing progress each second
	stop := util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"writeCount %d, sec %d, eps %d\n",
			writeCount, uint64(sec), writeCount / uint64(sec),
		)
	}, time.Second)

	wg := &sync.WaitGroup{}
	for i, ipIterator := range ipIterators {
		wg.Add(1)
		// reading each segment in separate goroutine
		go func (i int, ipIterator iter.Seq[uint32]) {
			defer wg.Done()
			var stageWG *sync.WaitGroup
			stage := 0

			// initializing current btree
			current[i] = btree.New[IP](btreeDegree)

			// prepare helper function which will move filled in-memory btree
			// into on-disk sorted array
			processStage := stageProcessor(pwd, i, &arrListPerStage[i])

			for ip := range ipIterator {
				atomic.AddUint64(&writeCount, 1)
				current[i].Put(IP(ip))

				// checking if btree is filled enough to store in on-disk array
				if current[i].Count() == elementsToRead {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount)
					// flushing btree data into on-disk array and creating new one
					stageWG = processStage(current[i])
					current[i] = btree.New[IP](btreeDegree)
					stage++
				}
			}

			// checking if processStage was executed at least once
			if stageWG != nil {
				// wait if previous stage processing didn't finished 
				stageWG.Wait()
			}

			// check if segment wasn't completely read and some in-memory data left
			if current[i].Count() != elementsToRead && current[i].Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount)
				// process rest data
				processStage(current[i]).Wait()
			}
		}(i, ipIterator)
	}

	wg.Wait() // waiting for ip file to be completely read
	stop()
	fmt.Println("writeCount -", writeCount)
	fmt.Println("============ READING PHASE ============")

	// count of read ip addresses from array files
	readCount := uint64(0)
	readCountPerSegment := make([]uint64, len(arrListPerStage))

	// count of unique ip addresses
	uniqCount := uint64(0)
	uniqCountPerSegment := make([]uint64, len(arrListPerStage))

	// printing progress each second
	stop = util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		readCount := uint64(0)
		uniqCount := uint64(0)
		for i := range len(arrListPerStage) {
			readCount += readCountPerSegment[i]
			uniqCount += uniqCountPerSegment[i]
		}

		fmt.Printf(
			"readCount %d, uniqCount %d, sec %d, eps %d\n",
			readCount, uniqCount, uint64(sec), readCount / uint64(sec),
		)
	}, time.Second)

	for i, arrList := range arrListPerStage {
		for j, a := range arrList {
			fmt.Printf("(%v,%v,%v),", i, j, a.Len())
		}
		fmt.Println()
	}

	// this two nested cycles are needed to distribute load on disk.
	// Actually just limits simultaneously running goroutines to parallelArrayReaderCount
	// It creates no more than parallelArrayReaderCount goroutines each of which
	// reads array lists created by index'th segment
	for i := range int(math.Ceil(float64(len(arrListPerStage)) / float64(parallelArrayReaderCount))) {
		wg := &sync.WaitGroup{}

		for j := range parallelArrayReaderCount {
			index := i * parallelArrayReaderCount + j
			if index == len(arrListPerStage) {
				break
			}

			last := IP(math.MaxUint32)
			arrList := arrListPerStage[index]
			iterators := make([]iter.Seq[IP], len(arrList))
			for i := range arrList {
				iterators[i] = arrList[i].Iterator(arrayIteratorCacheSize)
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

				atomic.AddUint64(&readCount, readCountPerSegment[index])
				atomic.AddUint64(&uniqCount, uniqCountPerSegment[index])
			}()
		}

		wg.Wait()
	}

	fmt.Println()
	fmt.Println("readCount -", readCount)
	fmt.Println("uniqCount -", uniqCount)
	fmt.Println("duration -", time.Since(start))
}
