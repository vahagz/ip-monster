package components

import (
	"fmt"
	"os"
	"path"
	"sync"

	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/util"
)

// returns helper function for converting btree into array
func stageProcessor(
	dstPath string,
	prefix string,
	i int,
	arrVirtualFileSize uint64,
	arrList *[]*Array,
) func(t *BTree) *sync.WaitGroup {
	m := &sync.Mutex{}
	arrayVFPool := &sync.Pool{New: func() any {
		vf := file.Virtual()
		vf.Truncate(arrVirtualFileSize)
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
				path.Join(dstPath, fmt.Sprintf("%s_%d_%d", prefix, i, len(*arrList))),
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
