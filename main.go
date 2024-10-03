package main

import (
	"bytes"
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

// folder where is located file with ip addresses.
const dataFolder = "data"

// name of the file with ips.
const ipFile = "ip_addresses.txt"
// const ipFile = "addreses - Copy.txt"

// folder where intermediate files will be placed.
const dstFolder = "dst";

// prefix for intermediate files created while counting.
const prefix = "array"

// parallel ip readers count. Each reader processes its own array files.
// readers are distributed linearly between ipFile.
const ipIteratorCount = 10

// count of goroutines reading final array files.
// Must be less or equal to ipIteratorCount
const parallelArrayReaderCount = 10

// count of elements to read for each iterator before processing to next stage.
const elementsToRead = 10_000_000

// minimal amount of data for read while reading ipFile.
const ipReaderPageSize = 4 * 1024 * 1024 // 4MB

// maximum count of ip addresses to store in memory while reading ipFile.
const ipReaderCacheSize = 1024

// degree of intermediate btree files.
// More degree - more disk space saving but slower insertion.
const btreeDegree = 10

// count of in-memory nodes while scanning btree
const treeIteratorCacheSize = 4 * btreeDegree

// minimal amount of data for read while reading sorted arrays
const arrayIteratorCacheSize = 1024 * 1024

// minimum count of bytes required to store ipv4.
// used to specify btree key size. IPs are stored as
// btree keys without duplicates.
const ipSize = int(unsafe.Sizeof(uint32(0)))

// size of in-memory array file
const arrayVirtualFileSize = elementsToRead * ipSize

type BTree = btree.BTree[IP]

type Array = array.Array[IP, *IP]

// btree key (aka ip). Implements btree.Key interface
type IP uint32

func (k IP) New() btree.Key  { return IP(0) }
func (k IP) Copy() btree.Key { return k }
func (k IP) Size() int       { return ipSize }
func (k IP) Compare(k2 util.Comparable) int {
	k2Casted := k2.(IP)
	if k < k2Casted {
		return -1
	} else if k > k2Casted {
		return 1
	}
	return 0
}

func stageProcessor(
	pwd string,
	i int,
	arrayVFPool *sync.Pool,
	arrList *[]Array,
) func(t *BTree) (*BTree, *sync.WaitGroup) {
	m := &sync.Mutex{}
	return func(t *BTree) (*BTree, *sync.WaitGroup) {
		wg := &sync.WaitGroup{}
		newTree := btree.New[IP](btreeDegree)

		wg.Add(1)
		go func () {
			defer wg.Done()
			m.Lock()
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
			for k := range t.Iterator(treeIteratorCacheSize) {
				arr.Push(&k)
			}

			// copying array in-memory data to file
			f.ReadFrom(bytes.NewBuffer(arr.File().Slice(0, uint64(arr.Len()) * uint64(ipSize))))

			// returning array virtual file to pool for reuse
			arrayVFPool.Put(arr.File().(*file.VirtualFile))

			util.PanicIfErr(f.Sync())
			*arrList = append(*arrList, array.New[IP](
				file.NewFromOSFile(f),
				t.Count(),
			))
		}()
	
		return newTree, wg
	}
}

func main() {
	start := time.Now()
	pwd := util.Must(os.Getwd())
	ipFile := util.Must(os.Open(path.Join(pwd, dataFolder, ipFile)))
	ipIterators := ip.Iterator(ipFile, ipReaderPageSize, ipReaderCacheSize, ipIteratorCount)
	writeCount := uint64(0)
	wg := &sync.WaitGroup{}
	arrListPerStage := make([][]Array, ipIteratorCount)
	current := make([]*BTree, ipIteratorCount)
	arrayVFPool := &sync.Pool{New: func() any {
		vf := file.New()
		vf.Truncate(uint64(arrayVirtualFileSize))
		return vf
	}}

	stop := util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"writeCount %d, sec %d, eps %d\n",
			writeCount, uint64(sec), writeCount / uint64(sec),
		)
	}, time.Second)

	for i, iterator := range ipIterators {
		wg.Add(1)
		go func (i int, iterator chan uint32) {
			defer wg.Done()
			var stageWG *sync.WaitGroup
			stage := 0
			current[i] = btree.New[IP](btreeDegree)
			processStage := stageProcessor(pwd, i, arrayVFPool, &arrListPerStage[i])

			for ip := range iterator {
				atomic.AddUint64(&writeCount, 1)
				current[i].Put(IP(ip))
				if current[i].Count() == elementsToRead {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount)
					current[i], stageWG = processStage(current[i])
					stage++
				}
			}

			if stageWG != nil {
				stageWG.Wait()
			}

			if current[i].Count() != elementsToRead && current[i].Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount)
				_, wg := processStage(current[i])
				wg.Wait()
			}
		}(i, iterator)
	}

	wg.Wait()
	stop()
	fmt.Println("==========================")
	fmt.Printf("writeCount %d\n", writeCount)

	// o := func(i, j int, length uint64) Array {
	// 	return array.New[IP](file.NewFromOSFile(util.Must(os.OpenFile(
	// 		path.Join(pwd, dataFolder, dstFolder, fmt.Sprintf("%s_%d_%d", prefix, i, j)),
	// 		os.O_RDONLY,
	// 		os.ModePerm,
	// 	))), length)
	// }
	// arrListPerStage = [][]Array{
	// 	{o(0,0,10000000),o(0,1,10000000),o(0,2,10000000),o(0,3,10000000),o(0,4,10000000),o(0,5,10000000),o(0,6,10000000),o(0,7,10000000),o(0,8,10000000),o(0,9,10000000),o(0,10,10000000),o(0,11,10000000),o(0,12,10000000),o(0,13,10000000),o(0,14,513482),},
	// 	{o(1,0,10000000),o(1,1,10000000),o(1,2,10000000),o(1,3,10000000),o(1,4,10000000),o(1,5,10000000),o(1,6,10000000),o(1,7,10000000),o(1,8,10000000),o(1,9,10000000),o(1,10,10000000),o(1,11,10000000),o(1,12,10000000),o(1,13,10000000),o(1,14,3217395),},
	// 	{o(2,0,10000000),o(2,1,10000000),o(2,2,10000000),o(2,3,10000000),o(2,4,10000000),o(2,5,10000000),o(2,6,10000000),o(2,7,10000000),o(2,8,10000000),o(2,9,10000000),o(2,10,10000000),o(2,11,10000000),o(2,12,10000000),o(2,13,10000000),o(2,14,8662188),},
	// 	{o(3,0,10000000),o(3,1,10000000),o(3,2,10000000),o(3,3,10000000),o(3,4,10000000),o(3,5,10000000),o(3,6,10000000),o(3,7,10000000),o(3,8,10000000),o(3,9,10000000),o(3,10,10000000),o(3,11,10000000),o(3,12,10000000),o(3,13,10000000),o(3,14,10000000),o(3,15,4488001),},
	// 	{o(4,0,10000000),o(4,1,10000000),o(4,2,10000000),o(4,3,10000000),o(4,4,10000000),o(4,5,10000000),o(4,6,10000000),o(4,7,10000000),o(4,8,10000000),o(4,9,10000000),o(4,10,10000000),o(4,11,10000000),o(4,12,10000000),o(4,13,10000000),o(4,14,10000000),o(4,15,8724455),},
	// 	{o(5,0,10000000),o(5,1,10000000),o(5,2,10000000),o(5,3,10000000),o(5,4,10000000),o(5,5,10000000),o(5,6,10000000),o(5,7,10000000),o(5,8,10000000),o(5,9,10000000),o(5,10,10000000),o(5,11,10000000),o(5,12,10000000),o(5,13,10000000),o(5,14,10000000),o(5,15,8307365),},
	// 	{o(6,0,10000000),o(6,1,10000000),o(6,2,10000000),o(6,3,10000000),o(6,4,10000000),o(6,5,10000000),o(6,6,10000000),o(6,7,10000000),o(6,8,10000000),o(6,9,10000000),o(6,10,10000000),o(6,11,10000000),o(6,12,10000000),o(6,13,10000000),o(6,14,10000000),o(6,15,1245593),},
	// 	{o(7,0,10000000),o(7,1,10000000),o(7,2,10000000),o(7,3,10000000),o(7,4,10000000),o(7,5,10000000),o(7,6,10000000),o(7,7,10000000),o(7,8,10000000),o(7,9,10000000),o(7,10,10000000),o(7,11,10000000),o(7,12,10000000),o(7,13,10000000),o(7,14,4674212),},
	// 	{o(8,0,10000000),o(8,1,10000000),o(8,2,10000000),o(8,3,10000000),o(8,4,10000000),o(8,5,10000000),o(8,6,10000000),o(8,7,10000000),o(8,8,10000000),o(8,9,10000000),o(8,10,10000000),o(8,11,10000000),o(8,12,10000000),o(8,13,9557916),},
	// 	{o(9,0,10000000),o(9,1,10000000),o(9,2,10000000),o(9,3,10000000),o(9,4,10000000),o(9,5,10000000),o(9,6,10000000),o(9,7,10000000),o(9,8,10000000),o(9,9,10000000),o(9,10,10000000),o(9,11,10000000),o(9,12,10000000),o(9,13,1783410),},
	// }

	readCount := uint64(0)
	uniqCount := uint64(0)
	stop = util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"readCount %d, uniqCount %d, sec %d, eps %d\n",
			readCount, uniqCount, uint64(sec), readCount / uint64(sec),
		)
	}, time.Second)

	for i, arrList := range arrListPerStage {
		for j, a := range arrList {
			fmt.Println(i, j, a.Len())
		}
	}

	cnt := int(math.Ceil(float64(len(arrListPerStage)) / float64(parallelArrayReaderCount)))
	for i := range cnt {
		wg := &sync.WaitGroup{}

		for j := range parallelArrayReaderCount {
			index := i * parallelArrayReaderCount + j
			if index == len(arrListPerStage) {
				break
			}

			wg.Add(1)
			go func (arrList []Array) {
				defer wg.Done()
				iterators := make([]iter.Seq[IP], len(arrList))
				for i := range arrList {
					iterators[i] = arrList[i].Iterator(arrayIteratorCacheSize)
				}

				last := IP(math.MaxUint32)
				for key := range util.MultIterator(iterators) {
					atomic.AddUint64(&readCount, 1)
					if last != key {
						last = key
						atomic.AddUint64(&uniqCount, 1)
					}
				}
			}(arrListPerStage[index])
		}

		wg.Wait()
	}

	fmt.Println()
	fmt.Println(readCount, uniqCount)
	fmt.Println(time.Since(start))
}
