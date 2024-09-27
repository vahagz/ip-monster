package ip

import (
	"bytes"
	"io"
	"os"
)

const MaxIpAddrSize = len("255.255.255.255\r\n")

func Iterator(file *os.File, pageSize int, chanSize int) <-chan []byte {
	ch := make(chan []byte, chanSize)
	buf := bytes.NewBuffer(make([]byte, 0, pageSize))

	go func ()  {
		for {
			ip, err := buf.ReadBytes('\n')
			if err == io.EOF {
				buf.Write(ip)
				buf.Grow(len(ip) + pageSize)
				b := buf.Bytes()
				b = b[:cap(b)]
				n, err := file.Read(b[len(ip):])
				buf = bytes.NewBuffer(b[:len(ip) + n])
				if err == io.EOF {
					break
				} else if err != nil {
					panic(err)
				}
			} else if err != nil {
				panic(err)
			} else {
				ip = ip[:len(ip) - 1]
				if ip[len(ip)-1] == '\r' {
					ip = ip[:len(ip) - 1]
				}
				ch <- ip
			}
		}
		close(ch)
	}()

	return ch
}
