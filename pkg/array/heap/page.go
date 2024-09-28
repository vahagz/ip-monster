package array

type page struct {
	id  uint64
	val []byte
}

func (p *page) Key() uint64 { return p.id }
