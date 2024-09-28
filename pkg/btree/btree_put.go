package btree

func (tree *BTree[I, K, KL, CL]) Put(key K) (inserted bool) {
	inserted = tree.insert(key)
	if inserted {
		tree.meta.Count++
	}
	return inserted
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
