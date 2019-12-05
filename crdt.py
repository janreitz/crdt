import abc
import enum
import asyncio
from copy import copy


class S4Vector():
    """
    ssn: Global session number
    pid: Process ID
    _sum: Sum(VectorTimestamp.value)
    seq: VectorTimestamp.value[pid]

    Globally unique, because _sum is unique to every site

    As defined in Replicated abstract data types: 
    Building blocks for collaborative applications
    """
    def __init__(self, ssn: int=0, pid: int=0, _sum: int=0, seq: int=0):
        self.ssn = ssn
        self.pid = pid
        self.sum = _sum
        self.seq = seq

    def __key(self):
        return (self.ssn, self.pid, self.sum, self.seq)
    
    def __eq__(self, other):
        return self.__key() == other.__key()

    def __hash__(self):
        return hash(self.__key())

    def __lt__(self, other):
        """
        S4Vectors are ordered by session number.
        In the same session, S4Vectors of a site are totally ordered
        because sum is monotonously increasing.
        In case of equal sums, ordering by pid 
        """
        if self.ssn < other.ssn: return True
        if self.ssn == other.ssn:
            if self.sum < other.sum: return True
            if self.sum == other.sum:
                if self.pid < other.pid: return True
        return False

    def __str__(self):
        return f"""ssn:{self.ssn}, pid:{self.pid}, sum:{self.sum}, seq:{self.seq}"""

class VectorTimestamp():
    """
    Is instantiated by a VectorClock() and should not be manipulated
    """
    def __init__(self, timestamp: [int]):
        self.value = timestamp

    def __le__(self, other) -> bool:
        for i, my_val in enumerate(self.value):
            if my_val > other.value[i]: return False
        return True

    def __lt__(self, other) -> bool:
        """
        A VectorTimestamp is smaller than another, if at least one
        element is smaller, while all others are smaller or equal.
        """
        at_least_one_less = False
        for i, my_val in enumerate(self.value):
            if my_val > other.value[i]: return False
            if my_val < other.value[i]: at_least_one_less = True
        return at_least_one_less

class VectorClock():
    """
    Every process has a vector clock, i.e. a vector 
    with a counter for each process including himself. 
    Whenever a process applies a local operation, 
    it increments the counter for its own process
    and multicasts a message with the operation
    and its timestamp, i.e. the current value of its
    vector clock. Other processes receiving this message
    can use the vector timestamp to infer if they are 
    ready to apply the remote operation to themselves,
    or if they have to wait for other remote operations, 
    which must be applied first (causal precedence). 
    When a process is ready to execute a remote operation 
    received from process B, it increments the counter 
    associated with process B and then executes it.
    """
    def __init__(self, num_processes: int):
        self.time = [0]*num_processes

    def __le__(self, other) -> bool:
        return self.time <= other.time

    def __lt__(self, other) -> bool:
        return self.time < other.time

    def __str__(self):
        return str(self.time)

    def increment(self, idx: int) -> bool:
        """
        Checks that idx is within bounds
        """
        if idx < 0 or idx > len(self.time): raise IndexError
        self.time[idx] += 1
        return True

    def decrement(self, idx: int) -> bool:
        """
        Checks that idx is within bounds
        """
        if idx < 0 or idx > len(self.time): raise IndexError
        self.time[idx] -= 1
        assert self.time[idx] >= 0
        return True

    def get_timestamp(self) -> VectorTimestamp:
        return VectorTimestamp(self.time.copy())

class OpType(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    INSERT = enum.auto()
    DELETE = enum.auto()
    UPDATE = enum.auto()
    REMOVE = enum.auto()
    PUT = enum.auto()

class Operation():
    def __init__(self, 
    op_type: OpType, 
    content: object=None):
        self.type = op_type
        self.content = content
        self.s4vec = None
        self.original_process_id = None
        self.timestamp = None

    def __str__(self):
        return f"""Type: {self.type}, 
        Content: {self.content}, 
        S4Vec: {self.s4vec},
        pid: {self.original_process_id}, 
        Timestamp: {self.timestamp}"""

class RFAReadOperation(Operation):
    """
    Content represents index
    """
    def __init__(self, 
    content: int):
        Operation.__init__(self, OpType.READ, content)
        
class RFAWriteOperation(Operation):
    """
    Content represents (index, value)
    """
    def __init__(self, 
    content: (int, object)):
        Operation.__init__(self, OpType.WRITE, content)

class RHTPutOperation(Operation):
    """
    Content represents (key, value)
    """
    def __init__(self, 
    content: (object, object)):
        Operation.__init__(self, OpType.PUT, content)

class RHTRemoveOperation(Operation):
    """
    Content represents key
    """
    def __init__(self, 
    content: object):
        Operation.__init__(self, OpType.REMOVE, content)

class RHTReadOperation(Operation):
    """
    Content represents key
    """
    def __init__(self, 
    content: object):
        Operation.__init__(self, OpType.READ, content)

class RGAReadOperation(Operation):
    """
    Content represents index
    """
    def __init__(self, 
    content: int):
        Operation.__init__(self, OpType.READ, content)

class RGALocalUpdateOperation(Operation):
    """
    Content represents (index, value)
    """
    def __init__(self, 
    content: (int, object)):
        Operation.__init__(self, OpType.UPDATE, content)

class RGARemoteUpdateOperation(Operation):
    """
    Content represents (index, value)
    """
    def __init__(self, 
    content: (S4Vector, object)):
        Operation.__init__(self, OpType.UPDATE, content)

class RGALocalDeleteOperation(Operation):
    """
    Content represents index
    """
    def __init__(self, 
    content: int):
        Operation.__init__(self, OpType.DELETE, content)

class RGARemoteDeleteOperation(Operation):
    """
    Content represents index
    """
    def __init__(self, 
    content: S4Vector):
        Operation.__init__(self, OpType.DELETE, content)

class RGALocalInsertOperation(Operation):
    """
    Content represents (index, value)
    """
    def __init__(self, 
    content: (int, object)):
        Operation.__init__(self, OpType.INSERT, content)

class RGARemoteInsertOperation(Operation):
    """
    Content represents (index, value)
    """
    def __init__(self, 
    content: (S4Vector, object)):
        Operation.__init__(self, OpType.INSERT, content)

class AbstractCRDT(abc.ABC):
    """
    """
    def __init__(self, ssn: int, num_processes: int, process_id: int, name: str=None):
        self.clock = VectorClock(num_processes)
        self.ssn = ssn
        self.process_id = process_id
        self.incoming_ops = asyncio.Queue() # Just received, never processed
        self.stored_ops = []                # Processed at least once and found causally unready at that time
        self.outgoing_ops = asyncio.Queue() # Local operation, already applied, ready for distribution
        self.name = name # For debug purposes
        self.__module__ = __file__

    def __str__(self):
        return f"""{self.name}, pid: {self.process_id}, time: {self.clock}
        Incoming: {self.incoming_ops.qsize()}, Stored: {len(self.stored_ops)}, Outgoing: {self.outgoing_ops.qsize()}"""

    async def enqueue_op(self, op: Operation):
        """
        Local changes should be applied by enqueueing Operations
        """
        await self.incoming_ops.put(op)

    def enqueue_op_nowait(self, op: Operation):
        """
        Local changes should be applied by enqueueing Operations
        """
        self.incoming_ops.put_nowait(op)

    def execute_local_op(self, op: Operation):
        """
        Bypasses the queue system. Not sure if this is a great idea,
        because it can lead to unexpected behavior. When prior local operations are enqueued
        and not yet processed, skipping the queue can mess up indexing.

        I recommend exclusively using either the queueing mechanism 
        or this for local operations
        """
        assert self.is_local_op(op)
        self._process_op_nowait(op)

    @abc.abstractmethod
    async def read(self, op: Operation) -> object:
        """
        Intended to be overwritten!
        """
        pass

    async def process_op(self, op):
        """
        Operation management -> Do not overwrite!
        Checks wether operation is causally ready and either applies or stores it.
        If a remote operation has been executed, all stored operations are checked for 
        causal readiness and are applied accordingly
        """
        if self.is_local_op(op) and op.type == OpType.READ: 
                # Reads don't need to be broadcast or recorded
                self._apply_local_op(op)

        elif self.is_local_op(op) and not op.type == OpType.READ: 
            self.clock.increment(self.process_id)
            if self._apply_local_op(op): await self._enqueue_outgoing_op(op)
            else: self.clock.decrement(self.process_id)

        elif not self.is_local_op(op):
            """
            A causally unready operation can become causally ready iff 
            the processes vector clock is incremented for a remote process, 
            i.e. a remote Operation is performed
            """    
            if self._causally_ready(op):
                """
                I think max(vec_clock_i[k], vec_clock_O[k]) can be omitted, since 
                vec_clock_i[k] =  vec_clock_O[k] + 1 is a neccessary condition for causal readiness 
                """
                self.clock.increment(op.original_process_id)
                self._apply_remote_op(op)
                # Apply all operations, who have become causally ready due to clock.increment()
                for op in filter(self._causally_ready, self.stored_ops):
                    self.clock.increment(op.original_process_id)
                    self._apply_remote_op(op)

            else: self.stored_ops.append(op)

    def _process_op_nowait(self, op):
        """
        Operation management -> Do not overwrite!
        Checks wether operation is causally ready and either applies or stores it.
        If a remote operation has been executed, all stored operations are checked for 
        causal readiness and are applied accordingly
        """
        if self.is_local_op(op) and op.type == OpType.READ: 
                # Reads don't need to be broadcast or recorded
                self._apply_local_op(op)

        elif self.is_local_op(op) and not op.type == OpType.READ: 
            self.clock.increment(self.process_id)
            if self._apply_local_op(op): self._enqueue_outgoing_op_nowait(op)
            else: self.clock.decrement(self.process_id)

        elif not self.is_local_op(op):
            """
            A causally unready operation can become causally ready iff 
            the processes vector clock is incremented for a remote process, 
            i.e. a remote Operation is performed
            """    
            if self._causally_ready(op):
                """
                I think max(vec_clock_i[k], vec_clock_O[k]) can be omitted, since 
                vec_clock_i[k] =  vec_clock_O[k] + 1 is a neccessary condition for causal readiness 
                """
                self.clock.increment(op.original_process_id)
                self._apply_remote_op(op)
                # Apply all operations, who have become causally ready due to clock.increment()
                for op in filter(self._causally_ready, self.stored_ops):
                    self.clock.increment(op.original_process_id)
                    self._apply_remote_op(op)

            else: self.stored_ops.append(op)

    async def start_consuming(self):
        """
        Continuously consume from incoming_ops
        """
        while True:
            op = await self.incoming_ops.get()
            await self.process_op(op)
            self.incoming_ops.task_done()

    async def process_queue(self):
        """
        Process incoming_ops until empty
        """
        while not self.incoming_ops.empty():
            op = await self.incoming_ops.get()
            await self.process_op(op)
            self.incoming_ops.task_done()

        assert self.incoming_ops.empty()

    def process_queue_nowait(self):
        """
        Process incoming_ops until empty
        """
        while not self.incoming_ops.empty():
            op = self.incoming_ops.get_nowait()
            self._process_op_nowait(op)
            self.incoming_ops.task_done()

        assert self.incoming_ops.empty()

    def _causally_ready(self, op: Operation) -> bool:
        """
        Determine if an operation is causally ready to be executed.
        Local operations always return True
        """
        if self.is_local_op(op): return True
        
        for i, local_time in enumerate(self.clock.time):
            if i == op.original_process_id:
                if op.timestamp.value[i] != local_time + 1: return False
            elif op.timestamp.value[i] > local_time: return False
        return True

    @abc.abstractmethod
    def _apply_local_op(self, op: Operation) -> bool:
        """
        Applies operation issued on-site to the internal data structure 
        -> Intended to be overwritten
        """
        return False

    @abc.abstractmethod
    def _apply_remote_op(self, op: Operation) -> bool:
        """
        Applies operation issued off-site to the internal data structure 
        -> Intended to be overwritten
        """
        return False

    def is_local_op(self, op: Operation) -> bool:
        return op.original_process_id is None

    def get_s4vec(self) -> S4Vector:
        
        current_time = self.clock.get_timestamp().value
        return copy(S4Vector(self.ssn, 
                self.process_id, 
                sum(current_time),
                current_time[self.process_id],
                ))

    def remote_op_from_local_op(self, local_op:Operation) -> Operation:
        """
        Attach site specific metadata to the local operation
        """
        local_op.s4vec = self.get_s4vec() 
        local_op.original_process_id = self.process_id
        local_op.timestamp = self.clock.get_timestamp()
        return local_op

    def serialize_op(self, op:Operation) -> object:
        """
        Intended to be overwritten
        """
        pass

    async def _enqueue_outgoing_op(self, op:Operation):
        """
        Attach metadata to Operation and place in queue outgoing_ops.
        """
        await self.outgoing_ops.put(self.remote_op_from_local_op(op))

    def _enqueue_outgoing_op_nowait(self, op:Operation):
        """
        Attach metadata to Operation and place in queue outgoing_ops.
        """
        self.outgoing_ops.put_nowait(self.remote_op_from_local_op(op))

class FixedSizeArray(AbstractCRDT):
    """
    Internally, elements are [obj, S4Vector]
    """
    def __init__(self, ssn: int, num_processes: int, process_id: int, size: int, name: str=None):
        AbstractCRDT.__init__(self, ssn, num_processes, process_id, name)
        self.array = [[None, S4Vector()] for _ in range(size)]

    def __str__(self):
        return AbstractCRDT.__str__(self) + f"""
        Current state {[element[0] for element in self.array]}"""
        #Current state {[[element[0], str(element[1])] for element in self.array]}"""

    def read(self, op: RFAReadOperation) -> object:
        if op.content >= 0 and op.content < len(self.array):
            return self.array[op.content][0]
        else: raise IndexError

    def get_array(self) -> [[object, S4Vector]]:
        return self.array

    def apply_local_op(self, op: Operation) -> bool:
        #print(f"{self.name}: Applying operation {op}. Current state: {self.array}")
        if op.type == OpType.WRITE: return self.local_write(op)
        else: raise NotImplementedError

    def apply_remote_op(self, op: Operation) -> bool:
        if op.type == OpType.WRITE: return self.remote_write(op)
        else: raise NotImplementedError
                
    def local_write(self, op: RFAWriteOperation) -> bool:
        idx = op.content[0]
        if idx < 0 and idx >= len(self.array): raise IndexError
        else:    
            self.array[idx][0] = op.content[1]
            self.array[idx][1] = self.get_s4vec()
            return True

    def remote_write(self, op: RFAWriteOperation) -> bool:
        idx = op.content[0]
        if self.array[idx][1] < op.s4vec:
            self.array[idx][0] = op.content[1]
            self.array[idx][1] = op.s4vec
            return True
        else: return False

class HashMap(AbstractCRDT):
    """
    Internally represented as dict -> {key: (object, S4Vector)}
    """
    def __init__(self, ssn: int, num_processes: int, process_id: int, name: str=None):
        AbstractCRDT.__init__(self, ssn, num_processes, process_id, name)
        self.hashmap = {}
        self.cemetery = {} # Tracks tombstones

    def __str__(self):
        return AbstractCRDT.__str__(self) + f"""
        Current state {self.hashmap}"""

    def __eq__(self, other: 'HashMap') -> bool:
        for i in self:
            if i not in other.hashmap: return False
            if self.hashmap[i][0] != other.hashmap[i][0]: return False
        return True

    def __hash__(self):
        hashes = []
        for i in self.hashmap:
            hashes.append(hash(self.hashmap[i][0]))
        hashes.sort()
        return hash(tuple(hashes))

    def __iter__(self):
        return iter(self.hashmap)

    def __getitem__(self, name):
        return self.hashmap[name]

    def keys(self):
        return self.hashmap.keys()

    def items(self):
        return self.hashmap.items()

    def values(self):
        return self.hashmap.values()

    def _apply_local_op(self, op: Operation) -> bool:
        #print(f"{self.name}: Applying operation {op}. Current state: {self.array}")
        if op.type == OpType.PUT: return self.local_put(op)
        elif op.type == OpType.REMOVE: return self.local_remove(op)
        else: raise NotImplementedError

    def _apply_remote_op(self, op: Operation) -> bool:
        #print(f"{self.name}: Applying operation {op}. Current state: {self.array}")
        if op.type == OpType.PUT: return self.remote_put(op)
        elif op.type == OpType.REMOVE: return self.remote_remove(op)
        else: raise NotImplementedError

    def read(self, op: RHTReadOperation) -> object:
        if op.content in self.hashmap: return self.hashmap[op.content]
        else: return None

    def local_remove(self, op: RHTRemoveOperation) -> bool:
        """
        Items are not really removed but set to None, in case
        a remote operation modifies it
        """
        if op.content not in self.hashmap: return False
        else: 
            self.hashmap[op.content] = (None, self.get_s4vec())
            self.cemetery[op.content] = (None, self.get_s4vec())
        return True

    def local_put(self, op: RHTPutOperation) -> bool:
        self.hashmap[op.content[0]] = (op.content[1], self.get_s4vec())
        return True

    def remote_remove(self, op: RHTRemoveOperation) -> bool:
        key = op.content[0]
        if key not in self.hashmap: raise KeyError
        if op.s4vec < self.hashmap[key][1]: return False
        # Item exists and Value is not Tombstone
        if self.hashmap[key][0]: self.cemetery[key] = (None, op.s4vec) 
        self.hashmap[key] = (None, op.s4vec) 
        return True

    def remote_put(self, op: RHTPutOperation) -> bool:
        s_o = op.s4vec
        key = op.content[0]
        value = op.content[1]
        # Item exists and remote op precedes current state
        if key in self.hashmap and op.s4vec < self.hashmap[key][1]:
            return False
        # Item exists and Value is None (Tombstone) -> resurrect, i.e. remove from cemetery
        elif key in self.hashmap and self.hashmap[key][0] is None: 
            del self.cemetery[key]
        # Item does not exist, or is ok to overwrite
        self.hashmap[key] = (value, s_o)
        return True

class RGANode():
    """
    s_k is used as key and precedence of inserts
    s_p is used for precedence of deletes and updates
    """
    def __init__(self, 
    obj: object=None,
    s_k: S4Vector=S4Vector(),
    s_p: S4Vector=S4Vector(), 
    next_: 'RGANode'=None, 
    link: 'RGANode'=None):
        self.obj = obj
        self.s_k = s_k
        self.s_p = s_p
        self.next = next_
        self.link = link

    def __str__(self):
        if self.obj == None: return ''
        return str(self.obj)

    def __hash__(self):
        """
        I think it's enough to hash this nodes properties, without 
        the recursive hash(self.link)
        This will be done if hash(LinkedList) is called
        self.s_p will differ among sites
        """
        return hash((self.obj, self.s_k))

class RGAIter():
    """
    Helper class for iterator protocol
    """
    def __init__(self, head: RGANode):
        self.node = head

    def __next__(self):
        if self.node is None: raise StopIteration
        node = self.node
        self.node = self.node.link
        return node

    def __hash__(self):
        return hash(self.node)

class RGALinkedList(AbstractCRDT):
    """
    Deleted items are kept as None, so-called tombstones.
    When indexing with n, the nth non-None value will be 
    accessed, instead of the nth value of the internal linked list.
    """
    def __init__(self, ssn: int, num_processes: int, process_id: int, name: str=None):
        AbstractCRDT.__init__(self, ssn, num_processes, process_id, name)
        self.head = None
        self.nodes_by_s4vector = {}
        self.cemetery = []

    def __str__(self):
        string = ""
        n = self.head
        i = 0
        while n is not None:
            string = f"{string}{i}: {str(n.obj)} -> "
            n = n.link
            i = i + 1
        return string

    def __len__(self) -> int:
        """
        Iterate list until last element, skipping over tombstones
        """
        n = self.head
        k = 0
        while(n is not None):
            if (n.obj is not None): k = k + 1 
            n = n.link
        return k

    def __iter__(self):
        return RGAIter(self.head)

    def __eq__(self, other):
        """
        Equal if all nodes have the same objects
        """
        for i, u in zip(self, other):
            if i.obj != u.obj: return False
        return True

    def __hash__(self):
        _hash_list = []
        for i in self:
            _hash_list.append(hash(i))
        return hash(tuple(_hash_list))

    def _apply_local_op(self, op: Operation):
        if op.type == OpType.DELETE: return self.local_delete(op)
        elif op.type == OpType.INSERT: return self.local_insert(op)
        elif op.type == OpType.UPDATE: return self.local_update(op)

    def _apply_remote_op(self, op: Operation):
        if op.type == OpType.DELETE: return self.remote_delete(op)
        elif op.type == OpType.INSERT: return self.remote_insert(op)
        elif op.type == OpType.UPDATE: return self.remote_update(op)
    
    def findlist(self, i):
        """
        Find ith node, skipping over tombstones (None values)
        """
        n = self.head
        k = 0
        while(n is not None):
            if (n.obj is not None): 
                if (i==k): return n
                k = k + 1 
            n = n.link
        return None

    def local_delete(self, op: RGALocalDeleteOperation) -> bool:
        target_n = self.findlist(op.content)
        if target_n is None: return False
        target_n.obj = None

        op.content = (target_n.s_k)
        return True

    def local_update(self, op: RGALocalUpdateOperation) -> bool:
        target_n = self.findlist(op.content[0])
        if target_n is None: return False
        target_n.obj = op.content[1]
        target_n.s_p = self.get_s4vec()
        op.content = (target_n.s_k, op.content[1])
        return True
    
    def read(self, op: RGAReadOperation) -> object:
        target_n = self.findlist(op.content)
        if target_n is None: return None
        return target_n.obj


    def local_insert(self, op: RGALocalInsertOperation) -> bool:
        """
        When head is None, Insertion at 0 initializes self.head
        """
        i = op.content[0]

        # Insertion at head
        if i == None: 
            curr_s4vec = self.get_s4vec()
            new_n = RGANode(op.content[1], curr_s4vec, curr_s4vec)
            self.nodes_by_s4vector[curr_s4vec] = new_n

            new_n.link = self.head
            self.head = new_n
            op.content = (None, op.content[1]) 
            return True

        # Insertion not at head
        refer_n = self.findlist(i)
        if refer_n is None: raise IndexError 

        curr_s4vec = self.get_s4vec()
        new_n = RGANode(op.content[1], curr_s4vec, curr_s4vec)
        self.nodes_by_s4vector[curr_s4vec] = new_n
        new_n.link = refer_n.link
        refer_n.link = new_n
        op.content = (refer_n.s_k, op.content[1])
        return True

    def remote_insert(self, op: RGARemoteInsertOperation) -> bool:
        i = op.content[0]   # S4Vec for indexing - None means head
        s_o = op.s4vec      # S4Vec of operation
        # (i) Find the left cobject in hash table
        if i is not None:
            ref = self.nodes_by_s4vector[i] # Raises KeyError
            while ref is not None and ref.s_k != i: ref = ref.next
            if ref is None: raise IndexError
        # (ii) Make new Node and insert into hashtable
        ins = RGANode(op.content[1], s_o, s_o)
        self.nodes_by_s4vector[s_o] = ins
        # (iii) Scan possible places
        if i is None:
            if self.head is None or self.head.s_k < ins.s_k:
                if self.head is not None: ins.link = self.head
                self.head = ins                
                return True
            else: ref = self.head
        # Iterate until either the end of the list, a tombstone, or an element with preceding s_k is found
        while ref.link is not None and ins.s_k < ref.link.s_k:
            ref = ref.link
        # (iv) Insert Node into list
        ins.link = ref.link
        ref.link = ins
        return True

    def remote_delete(self, op: RGARemoteDeleteOperation) -> bool:
        i = op.content
        s_o = op.s4vec
        n = self.nodes_by_s4vector[i]
        while n is not None and n.s_k != i: n = n.next
        if n is None: raise IndexError
        if n.obj is not None:
            n.obj = None
            n.s_p = s_o
            self.cemetery.append(n)
        return True

    def remote_update(self, op: RGARemoteUpdateOperation) -> bool:
        i = op.content[0]
        s_o = op.s4vec
        n = self.nodes_by_s4vector[i]
        while n is not None and n.s_k != i: n = n.next
        if n is None: raise IndexError
        if n.obj is None: return False
        if s_o < n.s_p: return False
        n.obj = op.content[1]
        n.s_p = s_o
        return True


class RGAList():
    """
    Variant with internal List. Probably YAGNI

    Deleted items are kept as None, so-called tombstones.
    When indexing with n, the nth non-None value will be 
    accessed, instead of the nth value of the internal list.
    """
    def __init__(self):
        self.list = [None]
        self.idx_by_s4vector = {}

    def apply_local_operations(self, ops: [Operation]):
        for op in ops:
            if op.type == OpType.READ: self.read(op)
            elif op.type == OpType.DELETE: self.local_delete(op)
            elif op.type == OpType.INSERT: self.local_insert(op)
            elif op.type == OpType.UPDATE: self.local_update(op)

    def apply_remote_operations(self, ops: [Operation]):
        for op in ops:
            if op.type == OpType.DELETE: self.remote_delete(op)
            elif op.type == OpType.INSERT: self.remote_insert(op)
            elif op.type == OpType.UPDATE: self.remote_update(op)
    
    def findlist(self, idx):
        """
        Find idx, skipping over None values
        """
        if idx < 0 or len(self.list) <= idx: raise IndexError
        i = 0
        for val in self.list:
            if val is not None:
                if i == idx: return i
                else: i = i + 1

    def read(self, op: RGAReadOperation) -> object:
        return self.list[self.findlist(op.content)]

    def local_delete(self, op: RGALocalDeleteOperation) -> bool:
        if self.list[self.findlist(op.content)] is None: 
            return False
        self.list[self.findlist(op.content)] = None
        return True

    def local_insert(self, op: RGALocalInsertOperation):
        """
        I need to touch this again!
        """
        if self.list[self.findlist(op.content[0])] is None: 
            return False
        self.list.insert(self.findlist(op.content[0]), op.content[1])
        return True

    def local_update(self, op: RGALocalUpdateOperation):
        if self.list[self.findlist(op.content[0])] is None: 
            return False
        self.list[self.findlist(op.content[0])] = op.content[1]
        return True

    def remote_insert(self, op:RGARemoteInsertOperation) -> bool:
        i = op.content[0]   # S4Vec for indexing - None means end of list
        s_o = op.s4vec      # S4Vec of operation
        ins = RGANode(op.content[1], op.s4vec, op.s4vec) # New node to be inserted
        ref = 0             # Index to be inserted after

        # (i) Find the left cobject in hash table
        if i is not None:
            ref = self.idx_by_s4vector[i] # Raises KeyError
        # (ii) Make new Node and insert into hashtable
        self.idx_by_s4vector[s_o] = ins
        # (iii) Scan possible places
        # Intention: append to list
        if i is None:
            if len(self.list) == 0 or self.list[0].s_k < ins.s_k:
                self.list.insert(0, ins)
                return True
            else: ref = 0
        # Iterate until either the end of the list, a tombstone, or an element with preceding s_k is found
        while self.list[ref] is not None and self.list[ref].s_k < ins.s_k:
            ref = ref + 1
        # (iv) Insert Node into list
        self.list.insert(ref + 1) # .insert() inserts before index, I want insert after ref

    def remote_delete(self, op:RGARemoteDeleteOperation) -> bool:
        pass
    def remote_update(self, op:RGARemoteUpdateOperation) -> bool:
        pass