package rbtree

import (
	"encoding/binary"
	"fmt"
	"math"

	a "ip_addr_counter/pkg/array"
	array "ip_addr_counter/pkg/array/generic"
	"ip_addr_counter/pkg/file"
	"ip_addr_counter/pkg/stack"
)

var bin = binary.BigEndian

func New[I a.Integer, K EntryItem](file file.Interface) *RBTree[I, K] {
	var n node[I, K]
	var k K
	arr := array.New[I, node[I, K]](file, n.size(), 0)

	nullNode := emptyNode[I, K]()
	nullNode.setBlack()
	nullPtr := arr.Push(nullNode)

	tree := &RBTree[I, K]{
		arr:  arr,
		meta: &Metadata[I]{
			nodeKeySize: uint16(k.Size()),
			root:        nullPtr,
			null:        nullPtr,
			count:       0,
		},
	}

	return tree
}

type RBTree[I a.Integer, K EntryItem] struct {
	arr  array.Array[I, node[I, K], *node[I, K]]
	meta *Metadata[I]
}

func (tree *RBTree[I, K]) Meta() *Metadata[I] {
	return tree.meta
}

func (tree *RBTree[I, K]) NodeSize() int {
	var n *node[I, K]
	return n.size()
}

func (tree *RBTree[I, K]) Put(key K) bool {
	node := newNode[I, K](key)
	node.left = tree.meta.null
	node.right = tree.meta.null
	node.setRed()
	inserted, err := tree.insert(node)
	if err != nil {
		panic(err)
	}
	return inserted
}

func (tree *RBTree[I, K]) Get(key K) bool {
	return tree.get(key) != tree.meta.null
}

func (tree *RBTree[I, K]) Delete(key K) {
	ptr := tree.get(key)
	if ptr == tree.meta.null {
		return
	}

	tree.arr.Get(ptr).key = key
	tree.delete(ptr)
}

func (tree *RBTree[I, K]) Scan(key K, scanFn func(key K) (bool, error)) error {
	if tree.meta.root == tree.meta.null {
		return nil
	}
	
	curr := tree.meta.root
	if !key.IsNil() {
		curr = tree.get(key)
	}

	s := stack.New[I](tree.height())
	for curr != 0 && curr != tree.meta.null || s.Size() > 0 {
		for curr != 0 && curr != tree.meta.null {
			s.Push(curr)
			if tree.arr.Get(curr).left == tree.meta.null {
				break
			}

			curr = tree.arr.Get(curr).left
		}

		curr = s.Pop()
		stop, err := scanFn(tree.arr.Get(curr).key)
		if stop || err != nil {
			return err
		}

		if tree.arr.Get(curr).right == tree.meta.null {
			curr = 0
		} else {
			curr = tree.arr.Get(curr).right
		}
	}

	return nil
}

func (tree *RBTree[I, K]) Count() int {
	return int(tree.meta.count)
}

func (tree *RBTree[I, K]) Print() error {
	return tree.print(tree.meta.root, 0, 2)
}

func (tree *RBTree[I, K]) print(root I, space int, shift int) error {
	if root == 0 {
		return nil
	}

	space += shift

	if root != tree.meta.null {
		tree.print(tree.arr.Get(root).right, space, shift)
	}

	fmt.Println()
	for i := shift; i < space; i++ {
		fmt.Print(" ")
	}

	fmt.Println(
		tree.arr.Get(root).key,
		tree.arr.Get(root).getFlag(FT_COLOR),
	)

	if root != tree.meta.null {
		tree.print(tree.arr.Get(root).left, space, shift)
	}
	return nil
}

func (tree *RBTree[I, K]) get(key K) I {
	ptr := tree.meta.root
	for ptr != tree.meta.null {
		switch key.Compare(tree.arr.Get(ptr).key) {
		case -1:
			ptr = tree.arr.Get(ptr).right
		case 1:
			ptr = tree.arr.Get(ptr).left
		default:
			return ptr
		}
	}
	return tree.meta.null
}

func (tree *RBTree[I, K]) height() int {
	return 2 * int(math.Ceil(math.Log2(float64(tree.meta.count)))) + 1
}

func (tree *RBTree[I, K]) fixDelete(x I) {
	for x != tree.meta.root && tree.arr.Get(x).isBlack() {
		if x == tree.arr.Get(tree.arr.Get(x).parent).left {
			w := tree.arr.Get(tree.arr.Get(x).parent).right

			if tree.arr.Get(w).isRed() { // case 1
				tree.arr.Get(w).setBlack()
				tree.arr.Get(tree.arr.Get(x).parent).setRed()

				tree.leftRotate(tree.arr.Get(x).parent)
				w = tree.arr.Get(tree.arr.Get(x).parent).right
			}

			if tree.arr.Get(tree.arr.Get(w).left).isBlack() && tree.arr.Get(tree.arr.Get(w).right).isBlack() { // case 2
				tree.arr.Get(w).setRed()
				x = tree.arr.Get(x).parent
			} else { // case 3, 4
				if tree.arr.Get(tree.arr.Get(w).right).isBlack() { // case 3
					tree.arr.Get(tree.arr.Get(w).left).setBlack()
					tree.arr.Get(w).setRed()

					tree.rightRotate(w)
					w = tree.arr.Get(tree.arr.Get(x).parent).right
				}

				// case 4
				tree.arr.Get(w).setFlag(FT_COLOR, tree.arr.Get(tree.arr.Get(x).parent).getFlag(FT_COLOR))
				tree.arr.Get(tree.arr.Get(x).parent).setBlack()
				tree.arr.Get(tree.arr.Get(w).right).setBlack()

				tree.leftRotate(tree.arr.Get(x).parent)
				x = tree.meta.root
			}
		} else {
			w := tree.arr.Get(tree.arr.Get(x).parent).left

			if tree.arr.Get(w).isRed() { // case 1
				tree.arr.Get(w).setBlack()
				tree.arr.Get(tree.arr.Get(x).parent).setRed()

				tree.rightRotate(tree.arr.Get(x).parent)
				w = tree.arr.Get(tree.arr.Get(x).parent).left
			}

			if tree.arr.Get(tree.arr.Get(w).right).isBlack() && tree.arr.Get(tree.arr.Get(w).left).isBlack() { // case 2
				tree.arr.Get(w).setRed()
				x = tree.arr.Get(x).parent
			} else { // case 3, 4
				if tree.arr.Get(tree.arr.Get(w).left).isBlack() { // case 3
					tree.arr.Get(tree.arr.Get(w).right).setBlack()
					tree.arr.Get(w).setRed()

					tree.leftRotate(w)
					w = tree.arr.Get(tree.arr.Get(x).parent).left
				}

				// case 4
				tree.arr.Get(w).setFlag(FT_COLOR, tree.arr.Get(tree.arr.Get(x).parent).getFlag(FT_COLOR))
				tree.arr.Get(tree.arr.Get(x).parent).setBlack()
				tree.arr.Get(tree.arr.Get(w).left).setBlack()

				tree.rightRotate(tree.arr.Get(x).parent)
				x = tree.meta.root
			}
		}
	}

	tree.arr.Get(x).setBlack()
}

func (tree *RBTree[I, K]) delete(z I) {
	var x I
	y := z
	yOriginalColor := tree.arr.Get(y).getFlag(FT_COLOR)

	if tree.arr.Get(z).left == tree.meta.null { // no children or only right
		x = tree.arr.Get(z).right
		tree.transplant(z, x)
	} else if tree.arr.Get(z).right == tree.meta.null { // only left child
		x = tree.arr.Get(z).left
		tree.transplant(z, x)
	} else { // both children
		y = tree.minimum(tree.arr.Get(z).right)
		yOriginalColor = tree.arr.Get(y).getFlag(FT_COLOR)
		x = tree.arr.Get(y).right

		if tree.arr.Get(y).parent == z { // y is direct child of z
			tree.arr.Get(x).parent = y
		} else {
			tree.transplant(y, x)
			tree.arr.Get(y).right = tree.arr.Get(z).right
			tree.arr.Get(tree.arr.Get(y).right).parent = y
		}

		tree.transplant(z, y)

		tree.arr.Get(y).left = tree.arr.Get(z).left
    tree.arr.Get(tree.arr.Get(y).left).parent = y
    tree.arr.Get(y).setFlag(FT_COLOR, tree.arr.Get(z).getFlag(FT_COLOR))
	}

	if yOriginalColor == FV_COLOR_BLACK {
		tree.fixDelete(x)
	}

	tree.free(z)
	tree.meta.count--
}

func (tree *RBTree[I, K]) minimum(x I) I {
	for tree.arr.Get(x).left != tree.meta.null {
		x = tree.arr.Get(x).left
	}
	return x
}

func (tree *RBTree[I, K]) transplant(u, v I) {
	if tree.arr.Get(u).parent == tree.meta.null { // u is root
		tree.meta.root = v
	} else {
		if u == tree.arr.Get(tree.arr.Get(u).parent).left { // u is left child
			tree.arr.Get(tree.arr.Get(u).parent).left = v
		} else { // u is right child
			tree.arr.Get(tree.arr.Get(u).parent).right = v
		}
	}

	tree.arr.Get(v).parent = tree.arr.Get(u).parent
}

func (tree *RBTree[I, K]) free(ptr I) {
	lastNodePtr := tree.arr.Len() - 1
	if ptr == lastNodePtr {
		tree.arr.Popn()
		return
	}

	// moving last node to freed space
	lastNode := tree.arr.Pop()
	if lastNodePtr == tree.arr.Get(lastNode.parent).left {
		tree.arr.Get(lastNode.parent).left = ptr
	} else {
		tree.arr.Get(lastNode.parent).right = ptr
	}

	tree.arr.Get(ptr).flags = lastNode.flags
	tree.arr.Get(ptr).left = lastNode.left
	tree.arr.Get(ptr).parent = lastNode.parent
	tree.arr.Get(ptr).right = lastNode.right
	tree.arr.Get(ptr).key = lastNode.key

	if tree.arr.Get(ptr).right != tree.meta.null {
		tree.arr.Get(tree.arr.Get(ptr).right).parent = ptr
	}
	
	if tree.arr.Get(ptr).left != tree.meta.null {
		tree.arr.Get(tree.arr.Get(ptr).left).parent = ptr
	}

	if lastNodePtr == tree.meta.root {
		tree.meta.root = ptr
	}
}
