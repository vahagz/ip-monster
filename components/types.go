package components

import (
	"unsafe"

	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/btree"
	"ip_addr_counter/pkg/util"
)

const ipSize = int(unsafe.Sizeof(IP(0)))

type WrtieConfigs struct {
	IPFilePath        string
	DstPath           string
	Prefix            string
	IPIteratorCount   int
	ElementsPerStage  int
	IPReaderPageSize  int
	IPReaderCacheSize int
	BTDegree          int
}

type ReadConfigs struct {
	ArrayListPerStage        [][]*Array
	ParallelArrayReaderCount int
	ArrayIteratorCacheSize   int
}

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
