// Searching a key on a B-tree in C++

class TreeNode {

friend class BTree;

private:
  int *keys;
  int t;
  TreeNode **C;
  int n;
  bool leaf;

public:
  TreeNode(int temp, bool bool_leaf);
  void insertNonFull(int k);
  void splitChild(int i, TreeNode *y);
  void traverse();
  TreeNode *search(int k);
};

TreeNode::TreeNode(int t1, bool leaf1) {
  t = t1;
  leaf = leaf1;

  keys = new int[2 * t - 1];
  C = new TreeNode *[2 * t];

  n = 0;
}

void TreeNode::traverse() {
  int i;
  for (i = 0; i < n; i++) {
    if (leaf == false)
      C[i]->traverse();
    // cout << " " << keys[i];
  }

  if (leaf == false)
    C[i]->traverse();
}

TreeNode *TreeNode::search(int k) {
  int i = 0;
  while (i < n && k > keys[i])
    i++;

  if (keys[i] == k)
    return this;

  if (leaf == true)
    return nullptr;

  return C[i]->search(k);
}

void TreeNode::insertNonFull(int k) {
  int i = this->n - 1;

  if (this->leaf == true) {
    while (i >= 0 && this->keys[i] > k) {
      this->keys[i + 1] = this->keys[i];
      i--;
    }

    this->keys[i + 1] = k;
    this->n = this->n + 1;
  } else {
    while (i >= 0 && this->keys[i] > k)
      i--;

    if (this->C[i + 1]->n == 2 * this->t - 1) {
      this->splitChild(i + 1, this->C[i + 1]);

      if (this->keys[i + 1] < k)
        i++;
    }
    this->C[i + 1]->insertNonFull(k);
  }
}

void TreeNode::splitChild(int i, TreeNode *y) {
  TreeNode *z = new TreeNode(y->t, y->leaf);
  z->n = this->t - 1;

  for (int j = 0; j < this->t - 1; j++)
    z->keys[j] = y->keys[j + this->t];

  if (y->leaf == false) {
    for (int j = 0; j < this->t; j++)
      z->C[j] = y->C[j + t];
  }

  y->n = this->t - 1;
  for (int j = this->n; j >= i + 1; j--)
    this->C[j + 1] = this->C[j];

  this->C[i + 1] = z;

  for (int j = n - 1; j >= i; j--)
    this->keys[j + 1] = this->keys[j];

  this->keys[i] = y->keys[t - 1];
  this->n = this->n + 1;
}













class BTree {
private:
  TreeNode *root;
  int t;

public:
  BTree(int temp) {
    root = nullptr;
    t = temp;
  }

  void traverse() {
    if (root != nullptr)
      root->traverse();
  }

  TreeNode *search(int k) {
    return (root == nullptr) ? nullptr : root->search(k);
  }

  void insert(int k);
};

void BTree::insert(int k) {
  if (root == nullptr) {
    root = new TreeNode(t, true);
    root->keys[0] = k;
    root->n = 1;
  } else {
    if (root->n == 2 * t - 1) {
      TreeNode *s = new TreeNode(t, false);

      s->C[0] = root;

      s->splitChild(0, root);

      int i = 0;
      if (s->keys[0] < k)
        i++;
      s->C[i]->insertNonFull(k);

      root = s;
    } else
      root->insertNonFull(k);
  }
}

int main() {
  BTree t(3);
  t.insert(8);
  t.insert(9);
  t.insert(10);
  t.insert(11);
  t.insert(15);
  t.insert(16);
  t.insert(17);
  t.insert(18);
  t.insert(20);
  t.insert(23);

  // cout << "The B-tree is: ";
  t.traverse();

  int k = 10;
  // (t.search(k) != nullptr) ? cout << endl
  //                << k << " is found"
  //             : cout << endl
  //                << k << " is not Found";

  k = 2;
  // (t.search(k) != nullptr) ? cout << endl
  //                << k << " is found"
  //             : cout << endl
  //                << k << " is not Found\n";
}