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

const dataFolder = "data"

// const dstFolder = "dst2"; const ipPath = "ip_addresses.xml"
const dstFolder = "dst"; const ipPath = "addreses - Copy.txt"
// const dstFolder = "dst"; const ipPath = "addreses2 - Copy.txt"

const ipPageSize = 4 * 1024 * 1024 // 4MB
const ipCacheSize = 1024
const ipIteratorCount = 10

// const tPath = "rbtree"
const tPath = "btree"

// const elementsToRead = 53_687_090 // == ~1GB rbtree file
// const elementsToRead = 55_924_053 // == ~1GB btree file if degree == 10
const elementsToRead = 10_000_000

const arrayIndexSize = 4
const ipSize = 4

const multiIteratorCacheSize = 50_000
const perTreeCacheSize = 10_000_000

const degree = 10
const maxChildCount = 2 * degree
const minChildCount = degree
const maxKeyCount = maxChildCount - 1
const minKeyCount = degree - 1

var maxNodeCount = int(math.Ceil(float64(elementsToRead) / float64(minKeyCount)))
var minNodeCount = int(math.Ceil(float64(elementsToRead) / float64(maxKeyCount)))
var nodeSize = btree.NodeSize[uint32, IP, KL, CL]()
var virtualFileSize = maxNodeCount * nodeSize

type KL    = [maxKeyCount * ipSize]byte
type CL    = [maxChildCount * arrayIndexSize]byte
type Meta  = btree.Metadata[uint32]
type BTree = btree.BTree[uint32, IP, KL, CL]

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

func processStage(
	m *sync.Mutex,
	pwd string,
	i int,
	vfPool *sync.Pool,
	treeArr *[]*BTree,
	t *BTree,
) (*BTree, *sync.WaitGroup) {
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
			path.Join(pwd, dataFolder, dstFolder, tPath + fmt.Sprintf("_%v_%v", i, len(*treeArr))),
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

func main() {
	pwd := util.Must(os.Getwd())
	ipFile := util.Must(os.Open(path.Join(pwd, dataFolder, ipPath)))
	ipIterators := ip.Iterator(ipFile, ipPageSize, ipCacheSize, ipIteratorCount)
	writeCount := uint64(0)
	start := time.Now()
	_ = start

	stop := util.SetInterval(func(start, now time.Time) {
		sec := now.Sub(start).Seconds()
		fmt.Printf(
			"writeCount %d, sec %d, weps %d\n",
			writeCount, uint64(sec), writeCount / uint64(sec),
		)
	}, time.Second)

	fmt.Println("nodeSize", btree.NodeSize[uint32, IP, KL, CL]())
	fmt.Println("maxNodeCount", maxNodeCount)
	fmt.Println("minNodeCount", minNodeCount)
	fmt.Println("virtualFile", virtualFileSize)
	treesPerStage := make([][]*BTree, ipIteratorCount)
	currentTrees := make([]*BTree, ipIteratorCount)
	vfPool := &sync.Pool{New: func() any {
		vf := file.New()
		vf.Truncate(uint64(virtualFileSize))
		return vf
	}}
	wg := &sync.WaitGroup{}
	wg.Add(len(ipIterators))

	for i, iterator := range ipIterators {
		go func (i int, iterator chan uint32) {
			defer wg.Done()
			m := &sync.Mutex{}
			stage := 0
			virtualFile := vfPool.Get().(*file.VirtualFile)
			currentTrees[i] = btree.New[uint32, IP, KL, CL](virtualFile, &Meta{Degree: degree})

			for ip := range iterator {
				atomic.AddUint64(&writeCount, 1)
				currentTrees[i].Put(IP(ip))
				if currentTrees[i].Count() == elementsToRead {
					fmt.Println("STAGE0", i, "|", stage, "|", writeCount, "|", currentTrees[i].NodeCount(), "|", *currentTrees[i].Meta())
					currentTrees[i], _ = processStage(m, pwd, i, vfPool, &treesPerStage[i], currentTrees[i])
					stage++
				}
			}

			if currentTrees[i].Count() != elementsToRead && currentTrees[i].Count() > 0 {
				fmt.Println("STAGE1", i, "|", stage, "|", writeCount, "|", currentTrees[i].NodeCount(), "|", *currentTrees[i].Meta())
				_, wg := processStage(m, pwd, i, vfPool, &treesPerStage[i], currentTrees[i])
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
			"readCount %d, uniqCount %d, sec %d, reps %d\n",
			readCount, uniqCount, uint64(sec), readCount / uint64(sec),
		)
	}, time.Second)

	for i, treeArr := range treesPerStage {
		for j, t := range treeArr {
			fmt.Println(i, j, t.Count(), t.Min(), t.Max(), t.Meta())
		}
	}

	// metasPerStage = [][]*btree.Metadata[uint32]{
	// 	{{10,10000000,352221}},
	// 	{{10,10000000,335658}},
	// 	{{10,10000000,366975}},
	// 	{{10,10000000,343976}},
	// 	{{10,10000000,360879}},
	// 	{{10,10000000,356779}},
	// 	{{10,10000000,327094}},
	// 	{{10,10000000,346502}},
	// 	{{10,10000000,351592}},
	// 	{{10,10000000,357243}},
	// }

	// for i, arr := range metasPerStage {
	// 	for j, meta := range arr {
	// 		fmt.Println(i, j, *meta)
	// 		treeArr = append(treeArr, btree.New[uint32, IP, KL, CL](file.NewFromOSFile(
	// 			util.Must(os.OpenFile(
	// 				path.Join(pwd, dataFolder, dstFolder, tPath + fmt.Sprintf("_%d_%d", i, j)),
	// 				os.O_RDONLY,
	// 				os.ModePerm,
	// 			)),
	// 		), meta))
	// 	}
	// }

	// iterables := make([]util.Iterable[IP], len(treeArr))
	// for i := range treeArr {
	// 	iterables[i] = treeArr[i]
	// }

	// last := IP(math.MaxUint32)
	// for key := range util.MultIterator(iterables, multiIteratorCacheSize, perTreeCacheSize) {
	// 	readCount++
	// 	if last != key {
	// 		last = key
	// 		uniqCount++
	// 	}
	// }

	// fmt.Println(readCount, uniqCount, time.Since(start))
}
