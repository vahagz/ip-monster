package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"ip_addr_counter/components"
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
const elementsPerStage = 10_000_000

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

func main() {
	start := time.Now()
	pwd := util.Must(os.Getwd())

	fmt.Println("============ WRITING PHASE ============")
	arrayListPerStage := components.Write(&components.WrtieConfigs{
		IPFilePath:        path.Join(pwd, dataFolder, ipFile),
		DstPath:           path.Join(pwd, dataFolder, dstFolder),
		Prefix:            prefix,
		IPIteratorCount:   ipIteratorCount,
		ElementsPerStage:  elementsPerStage,
		IPReaderPageSize:  ipReaderPageSize,
		IPReaderCacheSize: ipReaderCacheSize,
		BTDegree:          btreeDegree,
	})

	for i, arrList := range arrayListPerStage {
		for j, a := range arrList {
			fmt.Printf("(%v,%v,%v),", i, j, a.Len())
		}
		fmt.Println()
	}

	fmt.Println("============ READING PHASE ============")
	uniqCount := components.Read(&components.ReadConfigs{
		ArrayListPerStage:        arrayListPerStage,
		ParallelArrayReaderCount: parallelArrayReaderCount,
		ArrayIteratorCacheSize:   arrayIteratorCacheSize,
	})

	fmt.Println()
	fmt.Println("uniqCount -", uniqCount)
	fmt.Println("duration -", time.Since(start))
}
