package rbtree

import (
	"math"

	a "ip_addr_counter/pkg/array"
	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/stack"
)

func NewReader[I a.Integer, K Key](file file.Interface, meta *Metadata[I]) *RBTreeReader[I, K] {
	var n node[I, K]
	var arr array.Array[I, node[I, K], *node[I, K]]

	if meta == nil {
		arr = array.New[I, node[I, K]](file, n.size(), 0)
		nullNode := emptyNode[I, K]()
		nullNode.setBlack()
		nullPtr := arr.Push(nullNode)
		var k K
		meta = &Metadata[I]{
			NodeKeySize: uint16(k.Size()),
			Root:        nullPtr,
			Null:        nullPtr,
			Count:       0,
		}
	} else {
		arr = array.New[I, node[I, K]](file, n.size(), meta.Count + 1) // +1 for null node
	}

	tree := &RBTreeReader[I, K]{
		arr:  arr,
		meta: meta,
	}

	return tree
}

type RBTreeReader[I a.Integer, K Key] struct {
	arr  array.Array[I, node[I, K], *node[I, K]]
	meta *Metadata[I]
}

func (tree *RBTreeReader[I, K]) Meta() *Metadata[I] {
	return tree.meta
}

func (tree *RBTreeReader[I, K]) NodeSize() int {
	var n *node[I, K]
	return n.size()
}

func (tree *RBTreeReader[I, K]) Scan(key *K, scanFn func(key K) (stop bool, err error)) error {
	if tree.meta.Root == tree.meta.Null {
		return nil
	}

	curr := tree.meta.Root
	if key != nil {
		curr, _ = tree.searchIndex(*key)
	}

	s := stack.New[I](tree.height())
	for curr != 0 && curr != tree.meta.Null || s.Size() > 0 {
		for curr != 0 && curr != tree.meta.Null {
			s.Push(curr)
			if tree.arr.Get(curr).left == tree.meta.Null {
				break
			}

			curr = tree.arr.Get(curr).left
		}

		curr = s.Pop()
		stop, err := scanFn(tree.arr.Get(curr).key)
		if stop || err != nil {
			return err
		}

		if tree.arr.Get(curr).right == tree.meta.Null {
			curr = 0
		} else {
			curr = tree.arr.Get(curr).right
		}
	}

	return nil
}

func (tree *RBTreeReader[I, K]) Count() int {
	return int(tree.meta.Count)
}

func (tree *RBTreeReader[I, K]) Min() K {
	curr := tree.meta.Root
	currNode := tree.arr.Get(curr)
	for currNode.left != tree.meta.Null {
		curr = tree.arr.Get(curr).left
		currNode = tree.arr.Get(curr)
	}
	return currNode.key
}

func (tree *RBTreeReader[I, K]) Max() K {
	curr := tree.meta.Root
	currNode := tree.arr.Get(curr)
	for currNode.right != tree.meta.Null {
		curr = tree.arr.Get(curr).right
		currNode = tree.arr.Get(curr)
	}
	return currNode.key
}

func (tree *RBTreeReader[I, K]) height() int {
	return 2 * int(math.Ceil(math.Log2(float64(tree.meta.Count)))) + 1
}

func (tree *RBTreeReader[I, K]) searchIndex(key K) (I, bool) {
	lastGreaterPtr := tree.meta.Null
	index := tree.meta.Root
	for index != tree.meta.Null {
		switch tree.arr.Get(index).key.Compare(key) {
		case -1:
			index = tree.arr.Get(index).right
		case 1:
			lastGreaterPtr = index
			index = tree.arr.Get(index).left
		default:
			return index, true
		}
	}
	return lastGreaterPtr, false
}
