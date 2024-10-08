package ip

import (
	"bufio"
	"bytes"
	"math/rand/v2"
	"os"
	"strconv"

	"ip_addr_counter/pkg/util"
)

func Generate(dstPath string, count int, flags int) {
	f := util.Must(os.OpenFile(dstPath, os.O_RDWR|flags, os.ModePerm))
	bf := bufio.NewWriterSize(f, 16*4096)
	ip := &bytes.Buffer{}
	for range count {
		ip.Reset()
		ip.Write([]byte(strconv.Itoa(rand.Int() % 255)))
		ip.WriteByte('.')
		ip.Write([]byte(strconv.Itoa(rand.Int() % 255)))
		ip.WriteByte('.')
		ip.Write([]byte(strconv.Itoa(rand.Int() % 255)))
		ip.WriteByte('.')
		ip.Write([]byte(strconv.Itoa(rand.Int() % 255)))
		ip.WriteByte('\n')
		bf.Write(ip.Bytes())
	}
	util.PanicIfErr(bf.Flush())
	util.PanicIfErr(f.Close())
}
