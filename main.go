package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

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

// maximum count of ip addresses to store in memory
// while parallel reading intermediate btree files.
const multiIteratorCacheSize = 50_000

// maximum count of ip addresses to store in memory
// while reading individual btree file.
const perTreeCacheSize = 1_000_000

// max count of children pointers for each btree node.
// actually child of node is integer index of internal array
const maxChildCount = 2 * btreeDegree

// min count of children pointers for each btree node.
// actually child of node is integer index of internal array
const minChildCount = btreeDegree

// max count of keys for each btree node.
const maxKeyCount = maxChildCount - 1

// min count of keys for each btree node.
const minKeyCount = btreeDegree - 1

// count of in-memory nodes while scanning btree
const treeIteratorCacheSize = 2 * maxChildCount

// minimal amount of data for read while reading sorted arrays
const arrayIteratorPageSize = 4 * 1024 * 1024 // 4MB

const arrayIteratorCacheSize = 1000

// count of bytes for storing integers to internal on-disk array indexes.
// 4 bytes are sufficient while maxNodeCount for each intermediate btree
// fits in 4 byte unsigned integer. In the internal on-disk array contains
// all nodes of btree.
const arrayIndexSize = 4

// minimum count of bytes required to store ipv4.
// used to specify btree key size. IPs are stored as
// btree keys without duplicates.
const ipSize = 4

// max possible node count in each btree (worst case).
var maxNodeCount = int(math.Ceil(float64(elementsToRead) / float64(minKeyCount)))

// min possible node count in each btree (best case).
var minNodeCount = int(math.Ceil(float64(elementsToRead) / float64(maxKeyCount)))

// size of each node in bytes
var nodeSize = btree.NodeSize[uint32, IP, KL, CL]()

// size of in-memory files
var treeVirtualFileSize = maxNodeCount * nodeSize

const arrayVirtualFileSize = elementsToRead * ipSize

// type for specifying size of all keys in single node
type KL = [maxKeyCount * ipSize]byte

// type for specifying size of all children pointers in single node
type CL = [maxChildCount * arrayIndexSize]byte

type Meta = btree.Metadata[uint32]

type BTree = btree.BTree[uint32, IP, KL, CL]

type Array = array.Array[uint32, IP, *IP]

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
	treeVFPool, arrayVFPool *sync.Pool,
	arrList *[]Array,
) func(t *BTree) (*BTree, *sync.WaitGroup) {
	m := &sync.Mutex{}
	return func(t *BTree) (*BTree, *sync.WaitGroup) {
		wg := &sync.WaitGroup{}
		metaCopy := *t.Meta()
		metaCopy.Count = 0
		metaCopy.Root = 0
		newTree := btree.New[uint32, IP, KL, CL](treeVFPool.Get().(*file.VirtualFile), &metaCopy)

		wg.Add(1)
		go func () {
			defer wg.Done()
			m.Lock()
			defer m.Unlock()

			// initializing in-memory array to copy btree keys in increasing order
			arr := array.New[uint32, IP](arrayVFPool.Get().(*file.VirtualFile), ipSize, 0)

			// creating file for array
			f := util.Must(os.OpenFile(
				path.Join(pwd, dataFolder, dstFolder, fmt.Sprintf("%s_%d_%d", prefix, i, len(*arrList))),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				os.ModePerm,
			))

			// scanning btree and pushing to array
			t.Scan(treeIteratorCacheSize, func(k IP) {
				arr.Push(&k)
			})

			// returning btree virtual file to pool for reuse
			treeVFPool.Put(t.File().(*file.VirtualFile))

			// copying array in-memory data to file
			f.ReadFrom(bytes.NewBuffer(arr.File().Slice(0, uint64(arr.Len()) * ipSize)))

			// returning array virtual file to pool for reuse
			arrayVFPool.Put(arr.File().(*file.VirtualFile))

			util.PanicIfErr(f.Sync())
			*arrList = append(*arrList, array.New[uint32, IP](
				file.NewFromOSFile(f, arrayIteratorPageSize),
				ipSize,
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
	treeVFPool := &sync.Pool{New: func() any {
		vf := file.New()
		vf.Truncate(uint64(treeVirtualFileSize))
		return vf
	}}
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

	fmt.Println("nodeSize", btree.NodeSize[uint32, IP, KL, CL]())
	fmt.Println("maxNodeCount", maxNodeCount)
	fmt.Println("minNodeCount", minNodeCount)
	fmt.Println("virtualFile", treeVirtualFileSize)

	for i, iterator := range ipIterators {
		wg.Add(1)
		go func (i int, iterator chan uint32) {
			defer wg.Done()
			stage := 0
			virtualFile := treeVFPool.Get().(*file.VirtualFile)
			current[i] = btree.New[uint32, IP, KL, CL](virtualFile, &Meta{Degree: btreeDegree})
			processStage := stageProcessor(pwd, i, treeVFPool, arrayVFPool, &arrListPerStage[i])

			for ip := range iterator {
				atomic.AddUint64(&writeCount, 1)
				current[i].Put(IP(ip))
				if current[i].Count() == elementsToRead {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount, "|", current[i].NodeCount(), "|", *current[i].Meta())
					current[i], _ = processStage(current[i])
					stage++
				}
			}

			if current[i].Count() != elementsToRead && current[i].Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount, "|", current[i].NodeCount(), "|", *current[i].Meta())
				_, wg := processStage(current[i])
				wg.Wait()
			}
		}(i, iterator)
	}

	wg.Wait()
	fmt.Println("==========================")
	stop()

	// o := func(i, j int) Array {
	// 	return array.New[uint32, IP](file.NewFromOSFile(util.Must(os.OpenFile(
	// 		path.Join(pwd, dataFolder, dstFolder, fmt.Sprintf("%s_%d_%d", prefix, i, j)),
	// 		os.O_RDONLY,
	// 		os.ModePerm,
	// 	)), arrayIteratorPageSize), ipSize, 10_000_000)
	// }
	// arrListPerStage = [][]Array{
	// 	{o(0, 0),o(0, 1)},
	// 	{o(1, 0),o(1, 1)},
	// 	{o(2, 0),o(2, 1)},
	// 	{o(3, 0),o(3, 1)},
	// 	{o(4, 0),o(4, 1)},
	// 	{o(5, 0),o(5, 1)},
	// 	{o(6, 0),o(6, 1)},
	// 	{o(7, 0),o(7, 1)},
	// 	{o(8, 0),o(8, 1)},
	// 	{o(9, 0),o(9, 1)},
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
			fmt.Println(i, j, a.Len(), *a.Get(0), *a.Last())
		}
	}

	for i := range int(math.Ceil(float64(len(arrListPerStage)) / float64(parallelArrayReaderCount))) {
		wg := &sync.WaitGroup{}

		for j := range parallelArrayReaderCount {
			index := i * parallelArrayReaderCount + j
			if index == len(arrListPerStage) {
				break
			}

			wg.Add(1)
			go func (arrList []Array) {
				defer wg.Done()
				iterators := make([]<-chan IP, len(arrList))
				for i := range arrList {
					iterators[i] = arrList[i].Iterator(arrayIteratorCacheSize)
				}

				last := IP(math.MaxUint32)
				for key := range util.MultIterator(iterators, multiIteratorCacheSize) {
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

	fmt.Println(readCount, uniqCount)
	fmt.Println(time.Since(start))
}
