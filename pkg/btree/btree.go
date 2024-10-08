package btree

import (
	"iter"
)

func New[K Key](degree int) *BTree[K] {
	return &BTree[K]{degree: degree}
}

type BTree[K Key] struct {
	degree int
	count  uint64
	root   *node[K]
}

func (tree *BTree[K]) Put(key K) (inserted bool) {
	inserted = tree.insert(key)
	if inserted {
		tree.count++
	}
	return inserted
}

func (tree *BTree[K]) Count() uint64 {
	return tree.count
}

func (tree *BTree[K]) Height() int {
	height := 0
	n := tree.root
	for !n.isLeaf {
		height++
		n = n.children[0]
	}
	return height
}

func (tree *BTree[K]) Scan(fn func(k K) bool) {
	if tree.Count() == 0 {
		return
	}
	tree.traverse(tree.root, fn)
}

func (tree *BTree[K]) Iterator() iter.Seq[K] {
	return func(yield func(K) bool) {
		tree.Scan(func(k K) bool {
			return yield(k)
		})
	}
}

func (tree *BTree[K]) Min() K {
	curr := tree.root
	for !curr.isLeaf {
		curr = curr.children[0]
	}
	return curr.keys[0]
}

func (tree *BTree[K]) Max() K {
	curr := tree.root
	for !curr.isLeaf {
		curr = curr.children[curr.count]
	}
	return curr.keys[curr.count-1]
}

func (tree *BTree[K]) traverse(n *node[K], fn func(k K) bool) bool {
	for i := range n.count {
		if !n.isLeaf {
			if !tree.traverse(n.children[i], fn) {
				return false
			}
		}
		if !fn(n.keys[i]) {
			return false
		}
	}

	if !n.isLeaf {
		if !tree.traverse(n.children[n.count], fn) {
			return false
		}
	}
	return true
}

func (tree *BTree[K]) newNode(isLeaf bool) *node[K] {
	return newNode[K](tree.degree, isLeaf)
}

func (tree *BTree[K]) isFull(n *node[K]) bool {
	return n.count == 2*tree.degree-1
}

func (tree *BTree[K]) search(key K) (
	node *node[K],
	nodeIndex int,
	found bool,
) {
	node = tree.root
	nodeIndex, found = node.search(key)
	for !node.isLeaf {
		if found {
			return
		}
		node = node.children[nodeIndex]
		nodeIndex, found = node.search(key)
	}
	return
}

func (tree *BTree[K]) insert(key K) bool {
	if tree.Count() == 0 {
		root := newNode[K](tree.degree, true)
		root.keys[0] = key
		root.count = 1
		tree.root = root
		return true
	}

	node, _, found := tree.search(key)
	if found {
		return false
	} else if !tree.isFull(node) {
		return tree.insertNonFull(node, key)
	}

	if !tree.isFull(tree.root) {
		return tree.insertNonFull(tree.root, key)
	}

	s := newNode[K](tree.degree, false)
	s.children[0] = tree.root
	tree.splitChild(s, 0, tree.root)

	i := 0
	if s.keys[0].Compare(key) == -1 {
		i++
	}

	inserted := tree.insertNonFull(s.children[i], key)
	tree.root = s
	return inserted
}

func (tree *BTree[K]) insertNonFull(n *node[K], key K) bool {
	i, found := n.search(key)
	if found {
		return false
	}

	if n.isLeaf {
		i := n.count - 1
		for i >= 0 && n.keys[i].Compare(key) == 1 {
			n.keys[i+1] = n.keys[i]
			i--
		}

		n.keys[i+1] = key
		n.count++
		return true
	}

	c := n.children[i]
	_, found = c.search(key)
	if found {
		return false
	}

	if tree.isFull(c) {
		tree.splitChild(n, i, c)
		c = n.children[i]
		if n.keys[i].Compare(key) == -1 {
			c = n.children[i+1]
		}
	}
	return tree.insertNonFull(c, key)
}

func (tree *BTree[K]) splitChild(n *node[K], i int, y *node[K]) {
	z := tree.newNode(y.isLeaf)
	z.count = tree.degree - 1

	copy(z.keys, y.keys[tree.degree:])
	if !y.isLeaf {
		copy(z.children, y.children[tree.degree:])
	}

	y.count = tree.degree - 1
	copy(n.children[i+2:], n.children[i+1:n.count+1])

	n.children[i+1] = z

	copy(n.keys[i+1:], n.keys[i:n.count])

	n.keys[i] = y.keys[tree.degree-1]
	n.count++
}
