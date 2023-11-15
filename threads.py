import time


class PseudoTree:
    """
    simple tree, each node is integer value, where each parent node = sum if its children
    nonetheless, build logic is same as in dataheroes tree
    """
    def __init__(self, n_leaves):
        self.n_leaves = n_leaves
        self.tree = [[]]
        self.n_seconds_to_delay = .1
        self.leaf_factor = 2

    def _create_father_node(self, father_level, fs_idx):
        time.sleep(self.n_seconds_to_delay)
        return sum(self.tree[father_level-1][fs_idx: fs_idx + self.leaf_factor])
    
    def update_tree(self):
        level = 0  # Level
    
        # add new root if needed
        if len(self.tree[0]) == self.leaf_factor ** len(self.tree):
            self.tree.append([])
    
        # create father for each layer if it's full
        while len(self.tree[level]) % self.leaf_factor == 0:
            father_level = level + 1
            # first son idxs of the new father (rightmost one)
            fs_idx = len(self.tree[level]) - len(self.tree[level]) % self.leaf_factor - self.leaf_factor
            node = self._create_father_node(father_level, fs_idx)
            self.tree[father_level].append(node)
            level += 1

    def add_leaf(self, value):
        time.sleep(self.n_seconds_to_delay)
        self.tree[0].append(value)
        self.update_tree()

    def build(self):
        for i in range(self.n_leaves):
            tree.add_leaf(i)


class PseudoTreeParallel:
    """
    simple tree, each node is integer value, where each parent node = sum if its children
    nonetheless, build logic is same as in dataheroes tree
    """

    def __init__(self, n_leaves):
        self.n_leaves = n_leaves
        self.tree = [[]]
        self.n_seconds_to_delay = .1
        self.leaf_factor = 2

    def _create_father_node(self, father_level, fs_idx):
        time.sleep(self.n_seconds_to_delay)
        return sum(self.tree[father_level - 1][fs_idx: fs_idx + self.leaf_factor])

    def update_tree(self):
        level = 0  # Level

        # add new root if needed
        if len(self.tree[0]) == self.leaf_factor ** len(self.tree):
            self.tree.append([])

        # create father for each layer if it's full
        while len(self.tree[level]) % self.leaf_factor == 0:
            father_level = level + 1
            # first son idxs of the new father (rightmost one)
            fs_idx = len(self.tree[level]) - len(self.tree[level]) % self.leaf_factor - self.leaf_factor
            node = self._create_father_node(father_level, fs_idx)
            self.tree[father_level].append(node)
            level += 1

    def add_leaf(self, value):
        time.sleep(self.n_seconds_to_delay)
        self.tree[0].append(value)

    def build(self):
        for i in range(self.n_leaves):
            tree.add_leaf(i)
        self.update_tree()


#tree = PseudoTree(16)
tree = PseudoTreeParallel(16)
t = time.time()
tree.build()
print(f'Simple tree')
print(f'time for one node={tree.n_seconds_to_delay};\n{tree.n_leaves=}\nbuild time={time.time()-t:.2f}')
print(f'{tree.tree=}')