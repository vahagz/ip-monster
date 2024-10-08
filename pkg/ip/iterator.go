package ip

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"iter"
	"math"
	"os"
	"sync"

	"ip_addr_counter/pkg/util"
)

const MaxIpAddrSize = len("255.255.255.255\r\n")
const MinIpAddrSize = len("0.0.0.0\r\n")
const MaxIpAddrValue = math.MaxUint32

func Iterator(file *os.File, pageSize, cacheSize, count int) []iter.Seq[uint32] {
	wg := &sync.WaitGroup{}
	iterArr := make([]iter.Seq[uint32], count)
	chArr := make([]chan uint32, count)
	offsets := getOffsets(file, count)
	fileSize := util.Must(file.Stat()).Size()
	ctx, cancel := context.WithCancel(context.Background())

	for i := range count {
		ch := make(chan uint32, cacheSize)
		chArr[i] = ch
		iterArr[i] = func(yield func(uint32) bool) {
			for ip := range ch {
				if !yield(ip) {
					cancel()
					break
				}
			}
		}
	}

	for i := range count {
		ipParser := Parser()
		buf := bytes.NewBuffer(make([]byte, 0, pageSize))
		read := int64(0)
		from, to := int64(offsets[i]), int64(0)
		if i == count - 1 {
			to = fileSize
		} else {
			to = offsets[i + 1]
		}
		readCount := to - from

		wg.Add(1)
		go func () {
			defer wg.Done()
			for {
				ip, err := buf.ReadBytes('\n')
				if err == io.EOF {
					n, err := readPage(file, pageSize, buf, ip, from)
					from += int64(n)
					if n == 0 && err == io.EOF {
						if len(ip) > 0 {
							send(ctx, chArr, ipParser, ip)
						}
						break
					} else if err != io.EOF && err != nil {
						panic(err)
					}
				} else if err != nil {
					panic(err)
				} else {
					read += int64(len(ip))
					sent := send(ctx, chArr, ipParser, ip)
					if read >= readCount || !sent {
						break
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		cancel()
		for i := range count {
			close(chArr[i])
		}
	}()

	return iterArr
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

func send(ctx context.Context, chArr []chan uint32, parser *parser, ip []byte) bool {
	if ip[len(ip)-1] == '\n' {
		ip = ip[:len(ip) - 1]
	}
	if ip[len(ip)-1] == '\r' {
		ip = ip[:len(ip) - 1]
	}
	ipParsed := binary.BigEndian.Uint32(util.Must(parser.Parse(ip)))
	select {
	case <-ctx.Done():
		return false
	case chArr[getIndex(ipParsed, len(chArr))] <- ipParsed:
		return true
	}
}

func getOffsets(file *os.File, count int) []int64 {
	offsets := make([]int64, count)
	offsets[0] = 0

	fileSize := util.Must(file.Stat()).Size()
	sizePerIterator := fileSize / int64(count)
	
	b := make([]byte, MaxIpAddrSize)
	for i := 1; i < count; i++ {
		n, err := file.ReadAt(b, offsets[i - 1] + sizePerIterator)
		if err == io.EOF {
			b = b[:n]
		} else {
			util.PanicIfErr(err)
		}

		ip, err := bytes.NewBuffer(b).ReadBytes('\n')
		if err != io.EOF {
			util.PanicIfErr(err)
		}
		offsets[i] = offsets[i - 1] + sizePerIterator + int64(len(ip))
	}

	return offsets
}

func getIndex(ip uint32, count int) int {
	return int(float64(count) * float64(ip) / float64(MaxIpAddrValue + 1))
}
