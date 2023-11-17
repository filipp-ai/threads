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
                    print(f'{task_idx=}')
                    task_idx += 1
        if self.debug:
            print(f'no tasks in queue anymore! waiting to pool to be executed')
        self.pool.close()
        self.pool.join()


class PseudoTreeParallelLeavesOnly(PseudoTree):
    """
    simple tree, each node is integer value, where each parent node = sum if its children
    nonetheless, build logic is same as in dataheroes tree
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


class PseudoTreeParallelFull:
    """
    simple tree, each node is integer value, where each parent node = sum if its children
    nonetheless, build logic is same as in dataheroes tree
    """

    def __init__(self, n_leaves):
        self.executor = None
        self.n_leaves = n_leaves
        self.tree = [[]]
        self.n_seconds_to_delay = 1
        self.leaf_factor = 2
        self.queue = []

    def is_list_ready(self, required_node_list):
        for node in required_node_list:
            if node['level'] + 1 > len(self.tree):
                return False
            if node['node_idx'] + 1 > len(self.tree[node['level']]):
                return False
        return True

    def add_to_queue(self, required_node_list, call, args):
        print(f'add to queue! {args}')
        self.queue.append({'required_node_list': required_node_list, 'call': call, 'args': args})

    def check_queue(self):
        # exit if there are no tasks
        if len(self.queue) == 0:
            return 'FINISHED'
        for task in self.queue:
            if self.is_list_ready(task['required_node_list']):
                task['call'](task['args'])
                # EXEC
                return 'EXEC'
        # should wait
        return 'WAIT'

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
        print(f'add leaf {self=} {value=}')
        time.sleep(self.n_seconds_to_delay)
        self.tree[0].append(value)
        self.update_tree()

    def build(self):
        self.queue = []
        self.executor = Executor(n_nodes=1000, debug=True)
        for i in range(self.n_leaves):
            #tree.add_leaf(i)
            self.executor.add_to_queue(task={
                'condition': None,
                'call': self.add_leaf,
                'args':  [i]
            })
        self.executor.run()


total = []
executor = Executor(n_nodes=10, debug=True)


def handle_task(i, t):
    time.sleep(1)
    t.append(i)

    if (2*i > 8) and (2*i < 300):
        executor.add_to_queue(task={
            'condition': check_condition,
            'call': handle_task,
            'args': (2*i, total)
        })
    print(f'{executor.queue=}')
    print(f'{executor.pool._processes=}')

    return i

def check_condition(i, t):
    return True #and i == 0 or (i-1 in t)

"""
for i in range(100):
    executor.add_to_queue(task={
        'condition': check_condition,
        'call': handle_task,
        'args': (i, total)
    })

t = time.time()
print(f'{total}')
executor.run()
print(f'{total} {time.time()-t=}')
exit(0)
"""

tree = PseudoTreeParallelLeavesOnly(16)
t = time.time()
tree.build()
print(f'================{tree.__class__.__name__}================')
print(f'time for one node={tree.n_seconds_to_delay};\n{tree.n_leaves=}\nbuild time={time.time()-t:.2f}')
print(f'tree.tree=\n{tree.tree}'.replace('], [', '],\n ['))
