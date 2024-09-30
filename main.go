package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"
	"sync"
	"time"

	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/util"
)

const dataFolder = "data"

const dstFolder = "dst2"; const ipPath = "ip_addresses.xml"
// const dstFolder = "dst"; const ipPath = "addreses - Copy.txt"

const ipPageSize = 4 * 1024 * 1024 // 4MB
const ipCacheSize = 1024
const ipIteratorCount = 10

// const tPath = "rbtree"
const tPath = "btree"

// const elementsToRead = 53_687_090 // == ~1GB rbtree file
// const elementsToRead = 55_924_053 // == ~1GB btree file if degree == 10
const elementsToRead = 10_000_000

const arrayIndexSize = 4

const degree = 10
const maxChildCount = 2 * degree
const minChildCount = degree
const maxKeyCount = maxChildCount - 1
const minKeyCount = degree - 1

var maxNodeCount = int(math.Ceil(float64(elementsToRead) / float64(minKeyCount)))
var minNodeCount = int(math.Ceil(float64(elementsToRead) / float64(maxKeyCount)))
var nodeSize = btree.NodeSize[uint32, ip.IP, KL, CL]()
var virtualFileSize = maxNodeCount * nodeSize

type KL    = [maxKeyCount * ip.IpSize]byte
type CL    = [maxChildCount * arrayIndexSize]byte
type Meta  = btree.Metadata[uint32]
type BTree = btree.BTree[uint32, ip.IP, KL, CL]

func processStage(
	pwd string,
	i int,
	metaArr *[]*Meta,
	treeArr *[]*BTree,
	t *BTree,
) *BTree {
	tFile := util.Must(os.OpenFile(
		path.Join(pwd, dataFolder, dstFolder, tPath + fmt.Sprintf("_%v_%v", i, len(*metaArr))),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		os.ModePerm,
	))
	tFile.ReadFrom(bytes.NewBuffer(t.File().Slice(0, uint64(t.NodeCount()) * uint64(nodeSize))))
	util.PanicIfErr(tFile.Sync())

	*metaArr = append(*metaArr, t.Meta())
	*treeArr = append(*treeArr, btree.New[uint32, ip.IP, KL, CL](file.NewFromOSFile(tFile), t.Meta()))

	meta := *t.Meta()
	meta.Count = 0
	meta.Root = 0
	return btree.New[uint32, ip.IP, KL, CL](t.File(), &meta)
}

func main() {
	pwd := util.Must(os.Getwd())
	ipFile := util.Must(os.Open(path.Join(pwd, dataFolder, ipPath)))
	ipIterators := ip.Iterator(ipFile, ipPageSize, ipCacheSize, ipIteratorCount)
	writeCount := uint64(0)
	readCount := uint64(0)
	uniqCount := uint64(0)
	start := time.Now()

	go func() {
		for {
			time.Sleep(time.Second)
			sec := time.Since(start).Seconds()
			fmt.Printf(
				"writeCount %d, readCount %d, uniqCount %d, %d sec, %d weps, %d reps\n",
				writeCount, readCount, uniqCount, uint64(sec), writeCount / uint64(sec), readCount / uint64(sec),
			)
		}
	}()

	fmt.Println("nodeSize", btree.NodeSize[uint32, ip.IP, KL, CL]())
	fmt.Println("maxNodeCount", maxNodeCount)
	fmt.Println("minNodeCount", minNodeCount)
	fmt.Println("virtualFile", virtualFileSize)
	tMetas := make([][]*Meta, len(ipIterators))
	treeArr := []*BTree{}
	wg := sync.WaitGroup{}

	// for i, iterator := range ipIterators {
	// 	wg.Add(1)
	// 	go func (i int, iterator chan []byte)  {
	// 		defer wg.Done()
	// 		stage := 0
	// 		ipParser := ip.Parser()
	// 		virtualFile := file.New()
	// 		virtualFile.Truncate(uint64(virtualFileSize))
	// 		t := btree.New[uint32, ip.IP, KL, CL](virtualFile, &Meta{Degree: degree})
	// 		tMetas[i] = []*Meta{}

	// 		for itm := range iterator {
	// 			atomic.AddUint64(&writeCount, 1)
	// 			k := ip.IP(binary.BigEndian.Uint32(util.Must(ipParser.Parse(itm))))
	// 			t.Put(k)
	// 			if t.Count() == elementsToRead {
	// 				fmt.Println("STAGE0", i, "|", stage, "|", writeCount, "|", t.Count(), "|", t.NodeCount(), "|", *t.Meta(), "|", time.Since(start))
	// 				t = processStage(pwd, i, &tMetas[i], &treeArr, t)
	// 				stage++
	// 				return
	// 			}
	// 		}

	// 		if t.Count() != elementsToRead {
	// 			fmt.Println("STAGE1", i, "|", stage + 1, "|", writeCount, "|", t.Count(), "|", t.NodeCount(), "|", *t.Meta(), "|", time.Since(start))
	// 			processStage(pwd, i, &tMetas[i], &treeArr, t)
	// 		}
	// 	}(i, iterator)
	// }

	wg.Wait()
	start = time.Now().Add(-time.Second)
	last := ip.IP(math.MaxUint32)

	tMetas = [][]*btree.Metadata[uint32]{
		{{10,10000000,352221}},
		{{10,10000000,335658}},
		{{10,10000000,366975}},
		{{10,10000000,343976}},
		{{10,10000000,360879}},
		{{10,10000000,356779}},
		{{10,10000000,327094}},
		{{10,10000000,346502}},
		{{10,10000000,351592}},
		{{10,10000000,357243}},
	}

	for i, arr := range tMetas {
		for j, meta := range arr {
			fmt.Println(i, j, *meta)
			treeArr = append(treeArr, btree.New[uint32, ip.IP, KL, CL](file.NewFromOSFile(
				util.Must(os.OpenFile(
					path.Join(pwd, dataFolder, dstFolder, tPath + fmt.Sprintf("_%d_%d", i, j)),
					os.O_RDONLY,
					os.ModePerm,
				)),
			), meta))
		}
	}

	const multiIteratorCacheSize = 50000
	const perTreeCacheSize = 1000000
	for key := range btree.MultIterator(treeArr[:10], multiIteratorCacheSize, perTreeCacheSize) {
		readCount++
		if last != key {
			last = key
			uniqCount++
		}
	}

	fmt.Println(readCount, uniqCount, time.Since(start))
}
