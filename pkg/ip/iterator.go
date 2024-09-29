package ip

import (
	"bytes"
	"io"
	"os"

	"ip_addr_counter/pkg/util"
)

const MaxIpAddrSize = len("255.255.255.255\r\n")
const MinIpAddrSize = len("0.0.0.0\r\n")

func Iterator(file *os.File, pageSize, chanSize, count int) []chan []byte {
	chArr := make([]chan []byte, count)
	offsets := getOffsets(file, count)
	fileSize := util.Must(file.Stat()).Size()

	for i := range count {
		ch := make(chan []byte, chanSize)
		chArr[i] = ch
		from, to := int64(offsets[i]), int64(0)
		if i == count - 1 {
			to = fileSize
		} else {
			to = offsets[i + 1]
		}
		readCount := to - from

		go func (i int) {
			buf := bytes.NewBuffer(make([]byte, 0, pageSize))
			read := int64(0)
			for {
				ip, err := buf.ReadBytes('\n')
				if err == io.EOF {
					buf.Write(ip)
					buf.Grow(len(ip) + pageSize)
					b := buf.Bytes()
					b = b[:cap(b)]
					n, err := file.ReadAt(b[len(ip):], from)
					from += int64(n)
					buf = bytes.NewBuffer(b[:len(ip) + n])
					if err == io.EOF {
						if n == 0 {
							break
						}
					} else if err != nil {
						panic(err)
					}
				} else if err != nil {
					panic(err)
				} else {
					read += int64(len(ip))
					ip = ip[:len(ip) - 1]
					if ip[len(ip)-1] == '\r' {
						ip = ip[:len(ip) - 1]
					}
					ch <- ip
					if read >= readCount {
						break
					}
				}
			}
			close(ch)
		}(i)
	}

	return chArr
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
