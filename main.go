// {4 66   0 53687089}
// {4 8057 0 53687090}
// {4 952  0 53687090}
// {4 544  0 53687090}
// {4 4242 0 53687090}

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"time"

	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/util"
)

const dataFolder = "data"

const ipPath = "ip_addresses.xml"
// const ipPath = "addreses - Copy.txt"
const ipPageSize = 4 * 1024 * 1024 // 4MB
const ipCacheSize = 1024

// const tPath = "rbtree"
const tPath = "btree"

// const elementsToRead = 53_687_090 // == ~1GB rbtree file
// const elementsToRead = 15_000_000 // == ~1GB btree file
const elementsToRead = 50_000_000

const arrayIndexSize = 4

const degree = 10
const maxChildCount = 2 * degree
const minChildCount = degree
const maxKeyCount = maxChildCount - 1
const minKeyCount = degree - 1

var maxNodeCount = int(math.Ceil(float64(elementsToRead) / float64(minKeyCount)))
var minNodeCount = int(math.Ceil(float64(elementsToRead) / float64(maxKeyCount)))
var virtualFileSIze = maxNodeCount * btree.NodeSize[uint32, ip.IP, KL, CL]()

type KL [maxKeyCount * ip.IpSize]byte
type CL [maxChildCount * arrayIndexSize]byte


func processStage(
	pwd string,
	metaArr *[]*btree.Metadata[uint32],
	t *btree.BTree[uint32, ip.IP, KL, CL],
) *btree.BTree[uint32, ip.IP, KL, CL] {
	tFile := util.Must(os.OpenFile(
		path.Join(pwd, dataFolder, tPath + fmt.Sprintf("_%v", len(*metaArr))),
		os.O_RDWR|os.O_CREATE|os.O_TRUNC,
		os.ModePerm,
	))
	util.Must(t.File().WriteTo(tFile))
	util.PanicIfErr(tFile.Close())

	*metaArr = append(*metaArr, t.Meta())
	meta := t.Meta()
	meta.Count = 0
	meta.Root = 0
	return btree.New[uint32, ip.IP, KL, CL](t.File(), meta)
}

func main() {
	pwd := util.Must(os.Getwd())
	ipFile := util.Must(os.Open(path.Join(pwd, dataFolder, ipPath)))
	ipIterator := ip.Iterator(ipFile, ipPageSize, ipCacheSize)
	n := uint64(0)
	start := time.Now()

	go func() {
		for {
			time.Sleep(time.Second)
			offset := util.Must(ipFile.Seek(0, io.SeekCurrent))
			mb := float64(offset) / float64(1024 * 1024)
			sec := time.Since(start).Seconds()
			fmt.Printf(
				"elem %d, file %v mb, %d sec, %.2f avg mbps, %d eps, iter %d\n",
				n, uint64(mb), uint64(sec), mb / sec,
				n / uint64(sec), len(ipIterator),
			)
		}
	}()

	stage := 0
	ipParser := ip.Parser()
	virtualFile := file.New()
	virtualFile.Truncate(uint64(virtualFileSIze))
	t := btree.New[uint32, ip.IP, KL, CL](virtualFile, &btree.Metadata[uint32]{Degree: degree})
	tMetaArr := []*btree.Metadata[uint32]{}

	fmt.Println("nodeSize", btree.NodeSize[uint32, ip.IP, KL, CL]())
	fmt.Println("maxNodeCount", maxNodeCount)
	fmt.Println("minNodeCount", minNodeCount)
	fmt.Println("virtualFile", virtualFileSIze)

	for itm := range ipIterator {
		n++
		k := ip.IP(binary.BigEndian.Uint32(util.Must(ipParser.Parse(itm))))
		t.Put(k)
		if t.Count() == elementsToRead {
			fmt.Println(virtualFile.Size())
			fmt.Println("STAGE", stage + 1, t.Count(), *t.Meta(), time.Since(start))
			t = processStage(pwd, &tMetaArr, t)
			stage++
		}
	}

	if t.Count() != elementsToRead {
		fmt.Println("STAGE", stage + 1, t.Count(), *t.Meta(), time.Since(start))
		processStage(pwd, &tMetaArr, t)
	}

	for _, meta := range tMetaArr {
		fmt.Println(*meta)
	}




	// cnt := 0
	// max := IP(0)
	// util.SetInterval(func(start, now time.Time) {
	// 	fmt.Println(uint64(now.Sub(start).Seconds()), cnt)
	// }, time.Second)

	// rbtArr := []*rbtree.RBTreeReader[uint32, IP]{}
	// tMetaArr := []*rbtree.Metadata[uint32]{
	// 	{NodeKeySize: 4, Root: 66,   Null: 0, Count: 53687089},
	// 	{NodeKeySize: 4, Root: 8057, Null: 0, Count: 53687090},
	// 	{NodeKeySize: 4, Root: 952,  Null: 0, Count: 53687090},
	// 	{NodeKeySize: 4, Root: 544,  Null: 0, Count: 53687090},
	// 	{NodeKeySize: 4, Root: 4242, Null: 0, Count: 53687090},
	// }

	// for i := range tMetaArr {
	// 	rbtArr = append(rbtArr, rbtree.NewReader[uint32, IP](file.NewFromOSFile(
	// 		util.Must(os.OpenFile(
	// 			path.Join(pwd, dataFolder, tPath + fmt.Sprintf("_%d", i)),
	// 			os.O_RDONLY,
	// 			os.ModePerm,
	// 		)),
	// 	), tMetaArr[i]))
	// }

	// rbtArr[0].Scan(nil, func(key IP) (stop bool, err error) {
	// 	cnt++

	// 	if key > max {
	// 		max = key
	// 	}

	// 	return
	// })
	// fmt.Println(cnt)
	// fmt.Println(rbtArr[0].Max(), max)
}
