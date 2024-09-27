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
	"os"
	"path"
	"time"

	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/ip"
	"ip_addr_counter/pkg/rbtree"
)

const dataFolder = "data"

const ipPath = "ip_addresses.xml"
// const ipPath = "addreses - Copy.txt"
const ipPageSize = 4 * 1024 * 1024 // 4MB
const ipCacheSize = 1024

const rbtPath = "rbtree"

const elementsToRead = 53_687_090 // == ~1GB rbtree file

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

type IP uint32
func (k IP) New() rbtree.EntryItem { return IP(0) }
func (k IP) Copy() rbtree.EntryItem { return k }
func (k IP) Size() int { return 4 }
func (k IP) IsNil() bool { return k == 0 }
func (k IP) Compare(k2 rbtree.EntryItem) int {
	k2Casted := k2.(IP)
	if k < k2Casted {
		return -1
	} else if k > k2Casted {
		return 1
	}
	return 0
}
// func (k IP) MarshalBinary(into []byte) error { binary.BigEndian.PutUint32(into, uint32(k)); return nil }
// func (k *IP) UnmarshalBinary(data []byte) error { *k = IP(binary.BigEndian.Uint32(data)); return nil }

func main() {
	pwd, err := os.Getwd()
	panicIfErr(err)

	ipFile, err := os.Open(path.Join(pwd, dataFolder, ipPath))
	panicIfErr(err)

	ipIterator := ip.Iterator(ipFile, ipPageSize, ipCacheSize)
	n := uint64(0)
	start := time.Now()
	mbPrev := float64(0)
	stepsCount := 10
	mbPrevSteps := make([]float64, stepsCount)
	mbPrevStepsCursor := 0
	mbPrevSum := func () float64 {
		sum := float64(0)
		for _, mb := range mbPrevSteps {
			sum += mb
		}
		return sum
	}
	addPrevMb := func (mb float64) {
		mbPrevSteps[mbPrevStepsCursor] = mb
		mbPrevStepsCursor++
		if mbPrevStepsCursor == len(mbPrevSteps) {
			mbPrevStepsCursor = 0
		}
	}

	go func() {
		for {
			time.Sleep(time.Second)
			offset, err := ipFile.Seek(0, io.SeekCurrent)
			panicIfErr(err)
			mb := float64(offset) / float64(1024 * 1024)
			sec := time.Since(start).Seconds()
			addPrevMb(mb - mbPrev)
			mbPrev = mb
			fmt.Printf(
				"elem %d, file %v mb, %d sec, %.2f avg mbps, %.2f curr mbps, %d eps, iter %d\n",
				n, uint64(mb), uint64(sec), mb / sec, mbPrevSum() / float64(stepsCount),
				n / uint64(sec), len(ipIterator),
			)
		}
	}()

	virtualFile := file.New()
	virtualFile.Truncate(uint64((elementsToRead + 1) * rbtree.NodeSize[uint32, IP]()))
	rbt := rbtree.New[uint32, IP](virtualFile)
	rbtMetaArr := []*rbtree.Metadata[uint32]{}
	ipParser := ip.Parser()
	processStage := func () {
		rbtFile, err := os.OpenFile(
			path.Join(pwd, dataFolder, rbtPath + fmt.Sprintf("_%v", len(rbtMetaArr))),
			os.O_RDWR|os.O_CREATE|os.O_TRUNC,
			os.ModePerm,
		)
		panicIfErr(err)
		_, err = virtualFile.WriteTo(rbtFile)
		panicIfErr(err)
		panicIfErr(rbtFile.Close())

		rbtMetaArr = append(rbtMetaArr, rbt.Meta())
		rbt = rbtree.New[uint32, IP](virtualFile)
	}

	for itm := range ipIterator {
		n++
		parsed, err := ipParser.Parse(itm)
		panicIfErr(err)

		rbt.Put(IP(binary.BigEndian.Uint32(parsed)))

		if n % elementsToRead == 0 {
			fmt.Println("STAGE", n / elementsToRead, rbt.Count(), *rbt.Meta(), time.Since(start))
			processStage()
		}
	}

	if n % elementsToRead != 0 {
		fmt.Println("STAGE", 1 + n / elementsToRead, rbt.Count(), time.Since(start))
		processStage()
	}

	for _, meta := range rbtMetaArr {
		fmt.Println(*meta)
	}
}
