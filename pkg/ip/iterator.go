package ip

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"

	"ip_addr_counter/pkg/util"
)

const MaxIpAddrSize = len("255.255.255.255\r\n")
const MinIpAddrSize = len("0.0.0.0\r\n")
const MaxIpAddrValue = math.MaxUint32

func Iterator(file *os.File, pageSize, chanSize, count int) []chan uint32 {
	chArr := make([]chan uint32, count)
	offsets := getOffsets(file, count)
	fileSize := util.Must(file.Stat()).Size()
	wg := &sync.WaitGroup{}
	wg.Add(count)
	go func () {
		wg.Wait()
		for i := range count {
			close(chArr[i])
		}
	}()

	for i := range count {
		ch := make(chan uint32, chanSize)
		chArr[i] = ch
	}

	for i := range count {
		from, to := int64(offsets[i]), int64(0)
		if i == count - 1 {
			to = fileSize
		} else {
			to = offsets[i + 1]
		}
		readCount := to - from

		go func (i int) {
			defer wg.Done()
			ipParser := Parser()
			buf := bytes.NewBuffer(make([]byte, 0, pageSize))
			read := int64(0)
			for {
				ip, err := buf.ReadBytes('\n')
				if err == io.EOF {
					n, err := readPage(file, pageSize, buf, ip, from)
					from += int64(n)
					if n == 0 && err == io.EOF {
						break
					} else if err != io.EOF && err != nil {
						panic(err)
					}
				} else if err != nil {
					panic(err)
				} else {
					read += int64(len(ip))
					send(chArr, ipParser, ip)
					if read >= readCount {
						break
					}
				}
			}
		}(i)
	}

	return chArr
}

func readPage(file *os.File, pageSize int, buf *bytes.Buffer, halfReadIp []byte, from int64) (int, error) {
	buf.Write(halfReadIp)
	buf.Grow(len(halfReadIp) + pageSize)
	b := buf.Bytes()
	b = b[:cap(b)]
	n, err := file.ReadAt(b[len(halfReadIp):], from)
	*buf = *bytes.NewBuffer(b[:len(halfReadIp) + n])
	return n, err
}

func send(chArr []chan uint32, parser *parser, ip []byte) {
	ip = ip[:len(ip) - 1]
	if ip[len(ip)-1] == '\r' {
		ip = ip[:len(ip) - 1]
	}
	ipParsed := binary.BigEndian.Uint32(util.Must(parser.Parse(ip)))
	chArr[getIndex(ipParsed, len(chArr))] <- ipParsed
}

func getOffsets(file *os.File, count int) []int64 {
	offsets := make([]int64, count)
	offsets[0] = 0

	fileSize := util.Must(file.Stat()).Size()
	sizePerIterator := fileSize / int64(count)
	
	b := make([]byte, MaxIpAddrSize)
	for i := 1; i < count; i++ {
		util.Must(file.ReadAt(b, offsets[i - 1] + sizePerIterator))
		ip := util.Must(bytes.NewBuffer(b).ReadBytes('\n'))
		offsets[i] = offsets[i - 1] + sizePerIterator + int64(len(ip))
	}

	return offsets
}

func getIndex(ip uint32, count int) int {
	return int(float64(count) * float64(ip) / float64(MaxIpAddrValue + 1))
}
