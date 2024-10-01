package btree

import (
	a "ip_addr_counter/pkg/array"
	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/cache"
	"ip_addr_counter/pkg/file"
)

const ScanCacheSize = 10

func New[I a.Integer, K Key, KL, CL any](file file.Interface, meta *Metadata[I]) *BTree[I, K, KL, CL] {
	arr := array.New[I, nodeData[KL, CL]](file, NodeSize[I, K, KL, CL](), meta.Count)
	return &BTree[I, K, KL, CL]{
		arr:  arr,
		meta: meta,
	}
}

type BTree[I a.Integer, K Key, KL, CL any] struct {
	arr  array.Array[I, nodeData[KL, CL], *nodeData[KL, CL]]
	meta *Metadata[I]
}

func (tree *BTree[I, K, KL, CL]) Put(key K) (inserted bool) {
	inserted = tree.insert(key)
	if inserted {
		tree.meta.Count++
	}
	return inserted
}

func (tree *BTree[I, K, KL, CL]) Count() uint64 {
	return tree.meta.Count
}

func (tree *BTree[I, K, KL, CL]) Meta() *Metadata[I] {
	return tree.meta
}

func (tree *BTree[I, K, KL, CL]) File() file.Interface {
	return tree.arr.File()
}

func (tree *BTree[I, K, KL, CL]) Height() int {
	height := 0
	n := tree.get(tree.meta.Root)
	for !n.data.isLeaf {
		height++
		n = tree.get(n.children[0])
	}
	return height
}

func (tree *BTree[I, K, KL, CL]) NodeCount() I {
	return tree.arr.Len()
}

func (tree *BTree[I, K, KL, CL]) Scan(fn func(k K)) {
	if tree.Count() == 0 {
		return
	}

	c := cache.New[I, *node[I, K, KL, CL]](ScanCacheSize, nil)
	tree.traverse(c, tree.meta.Root, fn)
}

func (tree *BTree[I, K, KL, CL]) Iterator(cacheSize int) <-chan K {
	ch := make(chan K, cacheSize)
	go func () {
		tree.Scan(func(k K) {
			ch <- k
		})
		close(ch)
	}()
	return ch
}

func (tree *BTree[I, K, KL, CL]) Min() K {
	curr := tree.meta.Root
	currNode := tree.get(curr)
	for !currNode.data.isLeaf {
		curr = currNode.children[0]
		currNode = tree.get(curr)
	}
	return currNode.keys[0]
}

func (tree *BTree[I, K, KL, CL]) Max() K {
	curr := tree.meta.Root
	currNode := tree.get(curr)
	for !currNode.data.isLeaf {
		curr = currNode.children[currNode.data.count-1]
		currNode = tree.get(curr)
	}
	return currNode.keys[currNode.data.count-1]
}

func (tree *BTree[I, K, KL, CL]) traverse(c *cache.Cache[I, *node[I, K, KL, CL]], n I, fn func(k K)) {
	nNode, ok := c.Get(n)
	if !ok {
		nNode = tree.get(n)
		c.Add(n, nNode)
	}

	for i := range nNode.data.count {
    if !nNode.data.isLeaf {
			tree.traverse(c, nNode.children[i], fn)
		}
		fn(nNode.keys[i])
  }

	if !nNode.data.isLeaf {
		tree.traverse(c, nNode.children[nNode.data.count], fn)
	}

	tree.arr.Return(nNode.data)
}

func (tree *BTree[I, K, KL, CL]) get(i I) *node[I, K, KL, CL] {
	return tree.newNodeWithData(tree.arr.Get(i))
}

func (tree *BTree[I, K, KL, CL]) push(n *node[I, K, KL, CL]) I {
	return tree.arr.Push(n.data)
}

func (tree *BTree[I, K, KL, CL]) newNode(isLeaf bool) *node[I, K, KL, CL] {
	return newNode[I, K, KL, CL](tree.meta.Degree, isLeaf)
}

func (tree *BTree[I, K, KL, CL]) newNodeWithData(data *nodeData[KL, CL]) *node[I, K, KL, CL] {
	return newNodeWithData[I, K](data)
}

func (tree *BTree[I, K, KL, CL]) isFull(n *node[I, K, KL, CL]) bool {
	return n.data.count == 2*tree.meta.Degree-1
}

func (tree *BTree[I, K, KL, CL]) search(key K) (
	arrIndex I,
	node *node[I, K, KL, CL],
	nodeIndex int,
	found bool,
) {
	arrIndex = tree.meta.Root
	node = tree.get(arrIndex)
	nodeIndex, found = node.search(key)
	for !node.data.isLeaf {
		if found {
			return
		}
		arrIndex = node.children[nodeIndex]
		node = tree.get(arrIndex)
		nodeIndex, found = node.search(key)
	}
	return
}

func (tree *BTree[I, K, KL, CL]) insert(key K) bool {
	if tree.Count() == 0 {
		root := newNode[I, K, KL, CL](tree.meta.Degree, true)
		root.keys[0] = key
		root.data.count = 1
		tree.push(root)
		return true
	}

	arrIndex, node, _, found := tree.search(key)
	if found {
		return false
	} else if !tree.isFull(node) {
		return tree.insertNonFull(arrIndex, key)
	}

	rootNode := tree.get(tree.meta.Root)
	if !tree.isFull(rootNode) {
		return tree.insertNonFull(tree.meta.Root, key)
	}

	sNode := newNode[I, K, KL, CL](tree.meta.Degree, false)
	sNode.children[0] = tree.meta.Root
	s := tree.push(sNode)
	sNode = tree.get(s)
	tree.splitChild(s, 0, tree.meta.Root)

	i := 0
	if sNode.keys[0].Compare(key) == -1 {
		i++
	}

	inserted := tree.insertNonFull(sNode.children[i], key)
	tree.meta.Root = s
	return inserted
}

func (tree *BTree[I, K, KL, CL]) insertNonFull(n I, key K) bool {
	nNode := tree.get(n)
	i, found := nNode.search(key)
	if found {
		return false
	}

	if nNode.data.isLeaf {
		i := nNode.data.count - 1
		for i >= 0 && nNode.keys[i].Compare(key) == 1 {
			nNode.keys[i+1] = nNode.keys[i]
			i--
		}

		nNode.keys[i+1] = key
		nNode.data.count++
		return true
	}

	c := nNode.children[i]
	cNode := tree.get(c)
	_, found = cNode.search(key)
	if found {
		return false
	}

	if tree.isFull(cNode) {
		tree.splitChild(n, i, c)
		nNode = tree.get(n)
		c = nNode.children[i]
		if nNode.keys[i].Compare(key) == -1 {
			c = nNode.children[i+1]
		}
	}
	return tree.insertNonFull(c, key)
}

func (tree *BTree[I, K, KL, CL]) splitChild(n I, i int, y I) {
	nNode := tree.get(n)
	yNode := tree.get(y)
	zNode := tree.newNode(yNode.data.isLeaf)
	zNode.data.count = tree.meta.Degree - 1

	copy(zNode.keys, yNode.keys[tree.meta.Degree:])
	if !yNode.data.isLeaf {
		copy(zNode.children, yNode.children[tree.meta.Degree:])
	}

	yNode.data.count = tree.meta.Degree - 1
	copy(nNode.children[i+2:], nNode.children[i+1:nNode.data.count+1])

	z := tree.push(zNode)
	nNode.children[i+1] = z

	copy(nNode.keys[i+1:], nNode.keys[i:nNode.data.count])

	nNode.keys[i] = yNode.keys[tree.meta.Degree-1]
	nNode.data.count++
}
