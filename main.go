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

	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/util"
)

// folder where is located file with ip addresses.
const dataFolder = "data"

// name of the file with ips.
const ipFile = "ip_addresses.xml"
// const ipFile = "addreses - Copy.txt"

// folder where intermediate btree files will be placed.
const dstFolder = "dst";

// prefix for intermediate btree files created while counting.
const tPrefix = "btree"

// parallel ip readers count. Each reader processes its own btree files
// readers are distributed linearly between ipFile.
const ipIteratorCount = 10

// count of elements to read for each iterator before processing to next stage.
const elementsToRead = 10_000_000 // == ~203MB btree file if btreeDegree == 10

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

// size of in-memory file
var virtualFileSize = maxNodeCount * nodeSize

// type for specifying size of all keys in single node
type KL    = [maxKeyCount * ipSize]byte

// type for specifying size of all children pointers in single node
type CL    = [maxChildCount * arrayIndexSize]byte

// btree metadata type alias to avoid type parameter passing
type Meta  = btree.Metadata[uint32]

// btree type alias to avoid type parameter passing
type BTree = btree.BTree[uint32, IP, KL, CL]

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
	m *sync.Mutex,
	pwd string,
	i int,
	vfPool *sync.Pool,
	treeArr *[]*BTree,
) func(t *BTree) (*BTree, *sync.WaitGroup) {
	return func(t *BTree) (*BTree, *sync.WaitGroup) {
		wg := &sync.WaitGroup{}
		metaCopy := *t.Meta()
		metaCopy.Count = 0
		metaCopy.Root = 0
		newTree := btree.New[uint32, IP, KL, CL](vfPool.Get().(*file.VirtualFile), &metaCopy)
	
		wg.Add(1)
		go func () {
			defer wg.Done()
			m.Lock()
			defer m.Unlock()
	
			tFile := util.Must(os.OpenFile(
				path.Join(pwd, dataFolder, dstFolder, fmt.Sprintf("%s_%d_%d", tPrefix, i, len(*treeArr))),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				os.ModePerm,
			))
			tFile.ReadFrom(bytes.NewBuffer(t.File().Slice(0, uint64(t.NodeCount()) * uint64(nodeSize))))
			vfPool.Put(t.File().(*file.VirtualFile))
			util.PanicIfErr(tFile.Sync())
			*treeArr = append(*treeArr, btree.New[uint32, IP, KL, CL](file.NewFromOSFile(tFile), t.Meta()))
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
	treesPerStage := make([][]*BTree, ipIteratorCount)
	currentTrees := make([]*BTree, ipIteratorCount)
	vfPool := &sync.Pool{New: func() any {
		vf := file.New()
		vf.Truncate(uint64(virtualFileSize))
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
	fmt.Println("virtualFile", virtualFileSize)

	for i, iterator := range ipIterators {
		wg.Add(1)
		go func (i int, iterator chan uint32) {
			defer wg.Done()
			m := &sync.Mutex{}
			stage := 0
			virtualFile := vfPool.Get().(*file.VirtualFile)
			currentTrees[i] = btree.New[uint32, IP, KL, CL](virtualFile, &Meta{Degree: btreeDegree})
			processStage := stageProcessor(m, pwd, i, vfPool, &treesPerStage[i])

			for ip := range iterator {
				atomic.AddUint64(&writeCount, 1)
				currentTrees[i].Put(IP(ip))
				if currentTrees[i].Count() == elementsToRead {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount, "|", currentTrees[i].NodeCount(), "|", *currentTrees[i].Meta())
					currentTrees[i], _ = processStage(currentTrees[i])
					stage++
				}
			}

			if currentTrees[i].Count() != elementsToRead && currentTrees[i].Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount, "|", currentTrees[i].NodeCount(), "|", *currentTrees[i].Meta())
				_, wg := processStage(currentTrees[i])
				wg.Wait()
			}
		}(i, iterator)
	}

	wg.Wait()
	stop()

	readCount := uint64(0)
	uniqCount := uint64(0)
	stop = util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"readCount %d, uniqCount %d, sec %d, eps %d\n",
			readCount, uniqCount, uint64(sec), readCount / uint64(sec),
		)
	}, time.Second)

	for i, treeArr := range treesPerStage {
		for j, t := range treeArr {
			fmt.Println(i, j, t.Min(), t.Max(), t.Meta())
		}
	}

	for _, treeArr := range treesPerStage {
		iterables := make([]util.Iterable[IP], len(treeArr))
		for i := range treeArr {
			iterables[i] = treeArr[i]
		}

		last := IP(math.MaxUint32)
		for key := range util.MultIterator(iterables, multiIteratorCacheSize, perTreeCacheSize) {
			atomic.AddUint64(&readCount, 1)
			if last != key {
				last = key
				atomic.AddUint64(&uniqCount, 1)
			}
		}
	}

	fmt.Println(readCount, uniqCount, time.Since(start))
}
