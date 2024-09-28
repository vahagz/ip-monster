package btree

// ScanOptions tells bptree how to start tree scan
type ScanOptions struct {
	// if Key present, scan will start from given key
	Key [][]byte

	// if set true, scan will be in decreasing order on keys.
	Reverse bool

	// if set true, given key will be included in scan.
	Strict bool
}
