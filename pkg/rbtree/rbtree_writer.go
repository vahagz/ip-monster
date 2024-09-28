package rbtree

import (
	"encoding/binary"

	a "ip_addr_counter/pkg/array"
	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
)

var bin = binary.BigEndian

func NewWriter[I a.Integer, K Key](file file.Interface, meta *Metadata[I]) *RBTreeWriter[I, K] {
	arr := array.New[I, node[I, K]](file, NodeSize[I, K](), 0)
	nullNode := emptyNode[I, K]()
	nullNode.setBlack()
	nullPtr := arr.Push(nullNode)

	if meta == nil {
		meta = &Metadata[I]{
			NodeKeySize: uint16(KeySize[Key]()),
			Root:        nullPtr,
			Null:        nullPtr,
			Count:       0,
		}
	}
	tree := &RBTreeWriter[I, K]{
		arr:  arr,
		meta: meta,
	}

	return tree
}

type RBTreeWriter[I a.Integer, K Key] struct {
	arr  array.Array[I, node[I, K], *node[I, K]]
	meta *Metadata[I]
}

func (tree *RBTreeWriter[I, K]) Meta() *Metadata[I] {
	return tree.meta
}

func (tree *RBTreeWriter[I, K]) NodeSize() int {
	var n *node[I, K]
	return n.size()
}

func (tree *RBTreeWriter[I, K]) Put(key K) bool {
	node := newNode[I](key)
	node.left = tree.meta.Null
	node.right = tree.meta.Null
	node.setRed()
	inserted, err := tree.insert(node)
	if err != nil {
		panic(err)
	}
	return inserted
}

func (tree *RBTreeWriter[I, K]) Count() int {
	return int(tree.meta.Count)
}

func (tree *RBTreeWriter[I, K]) insert(zNode *node[I, K]) (bool, error) {
	y := tree.meta.Null
	temp := tree.meta.Root

	for temp != tree.meta.Null {
		y = temp
		switch zNode.key.Compare(tree.arr.Get(temp).key) {
		case -1:
			temp = tree.arr.Get(temp).left
		case 1:
			temp = tree.arr.Get(temp).right
		default:
			return false, nil
		}
	}

	zNode.parent = y
	zNode.left = tree.meta.Null
	zNode.right = tree.meta.Null
	z := tree.arr.Push(zNode)
	if y == tree.meta.Null {
		tree.meta.Root = z
	} else {
		switch zNode.key.Compare(tree.arr.Get(y).key) {
		case -1:
			tree.arr.Get(y).left = z
		default:
			tree.arr.Get(y).right = z
		}
	}

	tree.fixInsert(z)

	tree.meta.Count++
	return true, nil
}

func (tree *RBTreeWriter[I, K]) fixInsert(z I) {
	for tree.arr.Get(tree.arr.Get(z).parent).isRed() {
		if tree.arr.Get(z).parent == tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).left { // first 3 cases
			y := tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).right // z uncle

			// first subcase
			if tree.arr.Get(y).isRed() {
				tree.arr.Get(tree.arr.Get(z).parent).setBlack()
				tree.arr.Get(y).setBlack()
				tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).setRed()
				z = tree.arr.Get(tree.arr.Get(z).parent).parent
			} else { // second and third subcases
				if z == tree.arr.Get(tree.arr.Get(z).parent).right { // second subcase, turning to third
					z = tree.arr.Get(z).parent
					tree.leftRotate(z)
				}

				// third case
				tree.arr.Get(tree.arr.Get(z).parent).setBlack()
				tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).setRed()
				tree.rightRotate(tree.arr.Get(tree.arr.Get(z).parent).parent)
			}
		} else { // other 3 cases
			y := tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).left // z uncle

			// first subcase
			if tree.arr.Get(y).isRed() {
				tree.arr.Get(tree.arr.Get(z).parent).setBlack()
				tree.arr.Get(y).setBlack()
				tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).setRed()
				z = tree.arr.Get(tree.arr.Get(z).parent).parent
			} else { // second and third subcases
				if z == tree.arr.Get(tree.arr.Get(z).parent).left { // second subcase, turning to third
					z = tree.arr.Get(z).parent
					tree.rightRotate(z)
				}

				// third case
				tree.arr.Get(tree.arr.Get(z).parent).setBlack()
				tree.arr.Get(tree.arr.Get(tree.arr.Get(z).parent).parent).setRed()
				tree.leftRotate(tree.arr.Get(tree.arr.Get(z).parent).parent)
			}
		}
	}

	tree.arr.Get(tree.meta.Root).setBlack()
}

func (tree *RBTreeWriter[I, K]) leftRotate(x I) {
	y := tree.arr.Get(x).right

	tree.arr.Get(x).right = tree.arr.Get(y).left
	if tree.arr.Get(y).left != tree.meta.Null {
		tree.arr.Get(tree.arr.Get(y).left).parent = x
	}

	tree.arr.Get(y).parent = tree.arr.Get(x).parent

	if tree.arr.Get(x).parent == tree.meta.Null { // x is root
		tree.meta.Root = y
	} else {
		if tree.arr.Get(tree.arr.Get(x).parent).left == x { // x is left child
			tree.arr.Get(tree.arr.Get(x).parent).left = y
		} else { // x is right child
			tree.arr.Get(tree.arr.Get(x).parent).right = y
		}
	}

	tree.arr.Get(y).left = x
	tree.arr.Get(x).parent = y
}

func (tree *RBTreeWriter[I, K]) rightRotate(x I) {
	y := tree.arr.Get(x).left

	tree.arr.Get(x).left = tree.arr.Get(y).right
	if tree.arr.Get(y).right != tree.meta.Null {
		tree.arr.Get(tree.arr.Get(y).right).parent = x
	}

	tree.arr.Get(y).parent = tree.arr.Get(x).parent

	if tree.arr.Get(x).parent == tree.meta.Null { // x is root
		tree.meta.Root = y
	} else {
		if tree.arr.Get(tree.arr.Get(x).parent).right == x { // x is right child
			tree.arr.Get(tree.arr.Get(x).parent).right = y
		} else { // x is left child
			tree.arr.Get(tree.arr.Get(x).parent).left = y
		}
	}

	tree.arr.Get(y).right = x
	tree.arr.Get(x).parent = y
}
