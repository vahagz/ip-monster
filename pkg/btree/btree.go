package btree

import (
	a "ip_addr_counter/pkg/array"
	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
)

func New[I a.Integer, K Key, KL, CL any](file file.Interface, meta *Metadata[I]) *BTree[I, K, KL, CL] {
	arr := array.New[I, nodeData[KL, CL]](file, NodeSize[I, K, KL, CL](), 0)
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
	tree.traverse(tree.meta.Root, fn)
}

func (tree *BTree[I, K, KL, CL]) traverse(n I, fn func(k K)) {
	nNode := tree.get(n)
	for i := range nNode.data.count {
    if !nNode.data.isLeaf {
			tree.traverse(nNode.children[i], fn)
		}
		fn(nNode.keys[i])
  }

	if !nNode.data.isLeaf {
		tree.traverse(nNode.children[nNode.data.count], fn)
	}
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

func (tree *BTree[I, K, KL, CL]) insert(key K) bool {
	if tree.Count() == 0 {
		root := newNode[I, K, KL, CL](tree.meta.Degree, true)
		root.keys[0] = key
		root.data.count = 1
		tree.push(root)
		return true
	}

	rootNode := tree.get(tree.meta.Root)
	if rootNode.data.count < 2*tree.meta.Degree-1 {
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

	if cNode.data.count == 2*tree.meta.Degree-1 {
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

	for j := range tree.meta.Degree - 1 {
		zNode.keys[j] = yNode.keys[j+tree.meta.Degree]
	}

	if !yNode.data.isLeaf {
		for j := range tree.meta.Degree {
			zNode.children[j] = yNode.children[j+tree.meta.Degree]
		}
	}

	yNode.data.count = tree.meta.Degree - 1
	for j := nNode.data.count; j >= i+1; j-- {
		nNode.children[j+1] = nNode.children[j]
	}

	z := tree.push(zNode)
	nNode.children[i+1] = z

	for j := nNode.data.count - 1; j >= i; j-- {
		nNode.keys[j+1] = nNode.keys[j]
	}

	nNode.keys[i] = yNode.keys[tree.meta.Degree-1]
	nNode.data.count++
}
