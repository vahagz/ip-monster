package rbtree

func (tree *RBTree[I, K]) insert(zNode *node[I, K]) (bool, error) {
	y := tree.meta.null
	temp := tree.meta.root

	for temp != tree.meta.null {
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
	z := tree.arr.Push(*zNode)
	if y == tree.meta.null {
		tree.meta.root = z
	} else {
		switch zNode.key.Compare(tree.arr.Get(y).key) {
		case -1:
			tree.arr.Get(y).left = z
		default:
			tree.arr.Get(y).right = z
		}
	}

	zNode.left = tree.meta.null
	zNode.right = tree.meta.null

	tree.fixInsert(z)

	tree.meta.count++
	return true, nil
}

func (tree *RBTree[I, K]) fixInsert(z I) {
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

	tree.arr.Get(tree.meta.root).setBlack()
}

func (tree *RBTree[I, K]) leftRotate(x I) {
	y := tree.arr.Get(x).right

	tree.arr.Get(x).right = tree.arr.Get(y).left
	if tree.arr.Get(y).left != tree.meta.null {
		tree.arr.Get(tree.arr.Get(y).left).parent = x
	}

	tree.arr.Get(y).parent = tree.arr.Get(x).parent

	if tree.arr.Get(x).parent == tree.meta.null { // x is root
		tree.meta.root = y
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

func (tree *RBTree[I, K]) rightRotate(x I) {
	y := tree.arr.Get(x).left

	tree.arr.Get(x).left = tree.arr.Get(y).right
	if tree.arr.Get(y).right != tree.meta.null {
		tree.arr.Get(tree.arr.Get(y).right).parent = x
	}

	tree.arr.Get(y).parent = tree.arr.Get(x).parent

	if tree.arr.Get(x).parent == tree.meta.null { // x is root
		tree.meta.root = y
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
