package ip

import (
	"strconv"

	"ip_addr_counter/pkg/util"
)

type parser struct {
	num   []byte
	intIp []byte
}

func Parser() *parser {
	return &parser{
		num:   make([]byte, 0, 3),
		intIp: make([]byte, 0, 4),
	}
}

func (p *parser) Parse(src []byte) ([]byte, error) {
	p.intIp = p.intIp[:0]
	p.num = p.num[:0]

	for _, byt := range src {
		if byt != '.' {
			p.num = append(p.num, byt)
		} else {
			ipNumPart, err := strconv.ParseUint(util.BytesToString(p.num), 10, 8)
			if err != nil {
				return nil, err
			}

			p.intIp = append(p.intIp, byte(ipNumPart))
			p.num = p.num[:0]
		}
	}

	ipNumPart, err := strconv.ParseUint(util.BytesToString(p.num), 10, 8)
	if err != nil {
		return nil, err
	}

	p.intIp = append(p.intIp, byte(ipNumPart))
	return p.intIp, nil
}

