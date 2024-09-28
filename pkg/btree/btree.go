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

// BTree represents an on-disk bptree. Size of each node is
// decided based on key size, value size and tree degree.
type BTree[I a.Integer, K Key, KL, CL any] struct {
	arr  array.Array[I, nodeData[KL, CL], *nodeData[KL, CL]]
	meta *Metadata[I]
}

func (tree *BTree[I, K, KL, CL]) Count() uint64 {
	return tree.meta.Count
}

func (tree *BTree[I, K, KL, CL]) Meta() *Metadata[I] {
	return tree.meta
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
