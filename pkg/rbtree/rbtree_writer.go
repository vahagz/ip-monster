package rbtree

import (
	"encoding/binary"

	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
)

var bin = binary.BigEndian

func NewWriter[I Integer, K Key](file file.Interface, meta *Metadata[I]) *RBTreeWriter[I, K] {
	arr := array.New[node[I, K]](file, 0)
	nullNode := emptyNode[I, K]()
	nullNode.setBlack()
	nullPtr := arr.Push(nullNode)

	if meta == nil {
		meta = &Metadata[I]{
			NodeKeySize: uint16(KeySize[Key]()),
			Root:        I(nullPtr),
			Null:        I(nullPtr),
			Count:       0,
		}
	}
	tree := &RBTreeWriter[I, K]{
		arr:  arr,
		meta: meta,
	}

	return tree
}

type RBTreeWriter[I Integer, K Key] struct {
	arr  array.Array[node[I, K], *node[I, K]]
	meta *Metadata[I]
}

func (tree *RBTreeWriter[I, K]) Get(index I) *node[I, K] {
	return tree.arr.Get(uint64(index))
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
		switch zNode.key.Compare(tree.Get(temp).key) {
		case -1:
			temp = tree.Get(temp).left
		case 1:
			temp = tree.Get(temp).right
		default:
			return false, nil
		}
	}

	zNode.parent = y
	zNode.left = tree.meta.Null
	zNode.right = tree.meta.Null
	z := I(tree.arr.Push(zNode))
	if y == tree.meta.Null {
		tree.meta.Root = z
	} else {
		switch zNode.key.Compare(tree.Get(y).key) {
		case -1:
			tree.Get(y).left = z
		default:
			tree.Get(y).right = z
		}
	}

	tree.fixInsert(z)

	tree.meta.Count++
	return true, nil
}

func (tree *RBTreeWriter[I, K]) fixInsert(z I) {
	for tree.Get(tree.Get(z).parent).isRed() {
		if tree.Get(z).parent == tree.Get(tree.Get(tree.Get(z).parent).parent).left { // first 3 cases
			y := tree.Get(tree.Get(tree.Get(z).parent).parent).right // z uncle

			// first subcase
			if tree.Get(y).isRed() {
				tree.Get(tree.Get(z).parent).setBlack()
				tree.Get(y).setBlack()
				tree.Get(tree.Get(tree.Get(z).parent).parent).setRed()
				z = tree.Get(tree.Get(z).parent).parent
			} else { // second and third subcases
				if z == tree.Get(tree.Get(z).parent).right { // second subcase, turning to third
					z = tree.Get(z).parent
					tree.leftRotate(z)
				}

				// third case
				tree.Get(tree.Get(z).parent).setBlack()
				tree.Get(tree.Get(tree.Get(z).parent).parent).setRed()
				tree.rightRotate(tree.Get(tree.Get(z).parent).parent)
			}
		} else { // other 3 cases
			y := tree.Get(tree.Get(tree.Get(z).parent).parent).left // z uncle

			// first subcase
			if tree.Get(y).isRed() {
				tree.Get(tree.Get(z).parent).setBlack()
				tree.Get(y).setBlack()
				tree.Get(tree.Get(tree.Get(z).parent).parent).setRed()
				z = tree.Get(tree.Get(z).parent).parent
			} else { // second and third subcases
				if z == tree.Get(tree.Get(z).parent).left { // second subcase, turning to third
					z = tree.Get(z).parent
					tree.rightRotate(z)
				}

				// third case
				tree.Get(tree.Get(z).parent).setBlack()
				tree.Get(tree.Get(tree.Get(z).parent).parent).setRed()
				tree.leftRotate(tree.Get(tree.Get(z).parent).parent)
			}
		}
	}

	tree.Get(tree.meta.Root).setBlack()
}

func (tree *RBTreeWriter[I, K]) leftRotate(x I) {
	y := tree.Get(x).right

	tree.Get(x).right = tree.Get(y).left
	if tree.Get(y).left != tree.meta.Null {
		tree.Get(tree.Get(y).left).parent = x
	}

	tree.Get(y).parent = tree.Get(x).parent

	if tree.Get(x).parent == tree.meta.Null { // x is root
		tree.meta.Root = y
	} else {
		if tree.Get(tree.Get(x).parent).left == x { // x is left child
			tree.Get(tree.Get(x).parent).left = y
		} else { // x is right child
			tree.Get(tree.Get(x).parent).right = y
		}
	}

	tree.Get(y).left = x
	tree.Get(x).parent = y
}

func (tree *RBTreeWriter[I, K]) rightRotate(x I) {
	y := tree.Get(x).left

	tree.Get(x).left = tree.Get(y).right
	if tree.Get(y).right != tree.meta.Null {
		tree.Get(tree.Get(y).right).parent = x
	}

	tree.Get(y).parent = tree.Get(x).parent

	if tree.Get(x).parent == tree.meta.Null { // x is root
		tree.meta.Root = y
	} else {
		if tree.Get(tree.Get(x).parent).right == x { // x is right child
			tree.Get(tree.Get(x).parent).right = y
		} else { // x is left child
			tree.Get(tree.Get(x).parent).left = y
		}
	}

	tree.Get(y).right = x
	tree.Get(x).parent = y
}
