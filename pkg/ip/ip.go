package ip

import "ip_addr_counter/pkg/btree"

const IpSize = 4

type IP uint32

func (k IP) New() btree.Key  { return IP(0) }
func (k IP) Copy() btree.Key { return k }
func (k IP) Size() int       { return IpSize }
func (k IP) Compare(k2 btree.Key) int {
	k2Casted := k2.(IP)
	if k < k2Casted {
		return -1
	} else if k > k2Casted {
		return 1
	}
	return 0
}
