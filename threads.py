import threading
import time
from multiprocessing.pool import ThreadPool

N_SECONDS_TO_DELAY = 1
N_LEAF_FACTOR = 2


class PseudoTree:
    """
    simple tree, each node is integer value, where each parent node = sum if its children
    nonetheless, build logic is same as in dataheroes tree
    """
    def __init__(self, n_leaves):
        self.n_leaves = n_leaves
        self.tree = [[]]
        self.n_seconds_to_delay = N_SECONDS_TO_DELAY
        self.leaf_factor = N_LEAF_FACTOR

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


class Executor:
    def __init__(self, n_nodes=None, debug=False):
        self.n_nodes = n_nodes
        self.pool = ThreadPool(n_nodes)
        self.queue = []
        self.debug = debug
        self.running_processes = 0
        self.running_processes_max = self.pool._processes
        self.running_processes_lock = threading.Lock()

    def add_to_queue(self, task):
        with self.running_processes_lock:
            self.queue.append(task)

    def handle_error(self, e):
        raise e

    def finish_task(self, a):
        if self.debug:
            print(f'finish_task, {a=}')
        with self.running_processes_lock:
            self.running_processes -= 1
        if self.debug:
            print(f'finish_task, {self.running_processes=} {len(self.queue)=}')

    def run(self):
        while len(self.queue) > 0 or self.running_processes > 0:
            task_idx = 0
            run_a_task = False
            while not run_a_task and task_idx < len(self.queue):
                if self.debug:
                    print(f'{len(self.queue)=}; {task_idx=} {self.running_processes=} {self.running_processes_max=} ')
                task = self.queue[task_idx]
                if task['condition'] is None or task['condition'](*task['args']) \
                        and self.running_processes <= self.running_processes_max:
                    if self.debug:
                        print(f"put to pool task {task['args']}")
                    self.pool.apply_async(task['call'], task['args'],
                                          callback=self.finish_task,
                                          error_callback=self.handle_error,
                                          )
                    with self.running_processes_lock:
                        self.running_processes += 1
                        del self.queue[task_idx]
                    run_a_task = True
                else:
                    #print(f'{task_idx=}')
                    task_idx += 1
        if self.debug:
            print(f'no tasks in queue anymore! waiting to pool to be executed')
        self.pool.close()
        self.pool.join()


class PseudoTreeParallelLeavesOnly(PseudoTree):
    """
    parallel on the leaf level only
    """

    def __init__(self, n_leaves):
        self.executor = None
        super().__init__(n_leaves=n_leaves)

    def build(self):
        """
        parent implementation -
        for i in range(self.n_leaves):
            tree.add_leaf(i)
        """
        self.executor = Executor(1000)
        for i in range(self.n_leaves):
            self.executor.add_to_queue(task={
                'condition': None,
                'call': self.add_leaf,
                'args':  [i]
            })
        self.executor.run()


class PseudoTreeParallelFull(PseudoTree):
    """
    Fully parallel, that means that we could create nodes on all levels in different threads
    """

    def __init__(self, n_leaves):
        self.executor = None
        super().__init__(n_leaves=n_leaves)

    def _create_father_node_parallel(self, node):
        father_level, fs_idx = node
        node = self._create_father_node(father_level, fs_idx)
        with self.executor.running_processes_lock:
            if len(self.tree) < father_level + 1:
                self.tree.append([])
            self.tree[father_level].append(node)

    def add_leaf(self, value):
        time.sleep(self.n_seconds_to_delay)
        self.tree[0].append(value)
        # only create a leaf
        # self.update_tree()

    def _get_tree_by_n_leaves(self, n_leaves):
        result_tree = [[]]
        for i in range(n_leaves):
            result_tree[0].append(i)
            level = 0  # Level
            # add new root if needed
            if len(result_tree[0]) == self.leaf_factor ** len(result_tree):
                result_tree.append([])
            # create father for each layer if it's full
            while len(result_tree[level]) % self.leaf_factor == 0:
                father_level = level + 1
                # first son idxs of the new father (rightmost one)
                fs_idx = len(result_tree[level]) - len(result_tree[level]) % self.leaf_factor - self.leaf_factor
                result_tree[father_level].append(fs_idx)
                level += 1
        return result_tree

    def _get_diff_for_leaf(self, leaf_index):
        # leaf_index starts with 0
        tree_old = self._get_tree_by_n_leaves(leaf_index)
        tree_new = self._get_tree_by_n_leaves(leaf_index+1)
        new_nodes = []
        for level_idx, level in enumerate(tree_new):
            if level_idx > 0:
                if level_idx + 1 > len(tree_old):
                    new_nodes += [(level_idx, node_idx) for node_idx in tree_new[level_idx]]
                else:
                    new_nodes += [(level_idx, node_idx) for node_idx in tree_new[level_idx]
                                  if node_idx not in tree_old[level_idx]]
        return new_nodes

    def node_could_be_added(self, node):
        level_index = node[0]
        node_index = node[1]
        if len(self.tree) <= level_index - 1:
            # level does not exist yet
            return False
        if len(self.tree[level_index-1]) <= node_index + self.leaf_factor - 1:
            # children do not exist yet
            return False
        return True

    def build(self):
        self.executor = Executor(n_nodes=1000)
        for i in range(self.n_leaves):
            # add leaves
            self.executor.add_to_queue(task={
                'condition': None,
                'call': self.add_leaf,
                'args':  [i]
            })
            # add one task for each linked parent leaves
            nodes_to_add = self._get_diff_for_leaf(i)
            for node in nodes_to_add:
                self.executor.add_to_queue(task={
                    'condition': self.node_could_be_added,
                    'call': self._create_father_node_parallel,
                    'args': [node]
                })
        self.executor.run()


# ===========================================================================================
tree = PseudoTree(16)
t = time.time()
tree.build()
print(f'================{tree.__class__.__name__}================')
print(f'time for one node={tree.n_seconds_to_delay};\n{tree.n_leaves=}\nbuild time={time.time()-t:.2f}')
print(f'tree.tree=\n{tree.tree}'.replace('], [', '],\n ['))

tree = PseudoTreeParallelLeavesOnly(16)
t = time.time()
tree.build()
print(f'================{tree.__class__.__name__}================')
print(f'time for one node={tree.n_seconds_to_delay};\n{tree.n_leaves=}\nbuild time={time.time()-t:.2f}')
print(f'tree.tree=\n{tree.tree}'.replace('], [', '],\n ['))


tree = PseudoTreeParallelFull(16)
t = time.time()
tree.build()
print(f'================{tree.__class__.__name__}================')
print(f'time for one node={tree.n_seconds_to_delay};\n{tree.n_leaves=}\nbuild time={time.time()-t:.2f}')
print(f'tree.tree=\n{tree.tree}'.replace('], [', '],\n ['))
