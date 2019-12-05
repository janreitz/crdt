import random
import string
import asyncio
import crdt


async def print_states_rfa(rfas: [crdt.FixedSizeArray], sleep_time: float=0):
    while True:
        states = ["\n " for rfa in rfas]
        for i, rfa in enumerate(rfas):
            states[i] = states[i] + str(rfa)
        print(states)
        await asyncio.sleep(sleep_time)

async def make_n_random_local_changes_rfa(rfa: crdt.FixedSizeArray, n=10):
    for _ in range(n):
        await rfa.enqueue_op(random_local_rfa_write_op(rfa))

async def make_continuous_local_changes_rfa(rfa: crdt.FixedSizeArray, sleep_time: float=0):
    while True:
        await rfa.enqueue_op(random_local_rfa_write_op(rfa))
        await asyncio.sleep(sleep_time)

async def make_n_random_local_ops_rht(rht: crdt.HashMap, op_type: crdt.OpType=crdt.OpType.PUT, n: int=10):
    if op_type is None: random_type = True
    else: random_type = False
    for _ in range(n):
        if random_type: op_type = random.choice([crdt.OpType.PUT, crdt.OpType.REMOVE])
        if op_type == crdt.OpType.PUT: await rht.enqueue_op(random_local_rht_put_op(rht))
        elif op_type == crdt.OpType.REMOVE: await rht.enqueue_op(random_local_rht_remove_op(rht))
        else: raise NotImplementedError

async def make_n_random_local_ops_rga(rga: crdt.RGALinkedList, op_type: crdt.OpType=crdt.OpType.INSERT, n: int=10):
    if op_type is None: random_type = True
    else: random_type = False
    for _ in range(n):
        if random_type: op_type = random.choice([crdt.OpType.INSERT, crdt.OpType.UPDATE, crdt.OpType.DELETE])
        if op_type == crdt.OpType.INSERT: await rga.enqueue_op(random_local_rga_insert_op(rga))
        elif op_type == crdt.OpType.UPDATE: await rga.enqueue_op(random_local_rga_update_op(rga))
        elif op_type == crdt.OpType.DELETE: await rga.enqueue_op(random_local_rga_delete_op(rga))
        else: raise NotImplementedError

async def make_continuous_local_ops_rga(rga: crdt.RGALinkedList, sleep_time: float=0):
    while True:
        await rga.enqueue_op(random_local_rga_insert_op(rga))
        await asyncio.sleep(sleep_time)

def random_local_rfa_write_op(rfa: crdt.FixedSizeArray) -> crdt.RFAWriteOperation:
    len(rfa.array)
    index = random.randint(0,len(rfa.array) - 1)
    value = random.randint(0,99)
    return crdt.RFAWriteOperation((index, value))

def random_local_rht_put_op(rht: crdt.HashMap) -> crdt.RHTPutOperation:
    # Generate new key if hashmap is empty
    if len(rht.hashmap) == 0: key = random.choice(string.ascii_letters)
    # 50% generate new key
    elif random.choice([True, False]): key = random.choice(string.ascii_letters)
    # 50% pick existing
    else: key = random.choice(list(rht.hashmap.keys()))
    value = random.randint(0,99)
    return crdt.RHTPutOperation((key, value))

def random_local_rht_remove_op(rht: crdt.HashMap) -> crdt.RHTRemoveOperation:
    key = random.choice(list(rht.hashmap.keys()))
    return crdt.RHTRemoveOperation(key)

def random_local_rga_insert_op(rga: crdt.RGALinkedList) -> crdt.RGALocalInsertOperation:
    if (len(rga)) == 0: idx = None
    else: idx = random.choice(range(len(rga)))
    content = random.choice(string.ascii_letters)
    return crdt.RGALocalInsertOperation((idx, content))

def random_local_rga_update_op(rga: crdt.RGALinkedList) -> crdt.RGALocalUpdateOperation:
    if (len(rga)) == 0: raise IndexError
    else: idx = random.choice(range(len(rga)))
    content = random.choice(string.ascii_letters)
    return crdt.RGALocalUpdateOperation((idx, content))

def random_local_rga_delete_op(rga: crdt.RGALinkedList) -> crdt.RGALocalDeleteOperation:
    if (len(rga)) == 0: raise IndexError
    else: idx = random.choice(range(len(rga)))
    return crdt.RGALocalDeleteOperation(idx)

async def simulate_network(crdts: [crdt.AbstractCRDT]):
    """
    Simulate a network connection, by distributing all operations continuously
    """
    while True:
        for curr_crdt in crdts:
            if not curr_crdt.outgoing_ops.empty():
                op = await curr_crdt.outgoing_ops.get()
                # put in all other incoming_ops
                for other_crdt in list(filter(lambda other: other != curr_crdt, crdts)):
                    await other_crdt.enqueue_op(op)
                curr_crdt.outgoing_ops.task_done()
            else: await asyncio.sleep(0.1)

async def simulate_sync(crdts: [crdt.AbstractCRDT]):
    """
    Simulate synchronization, by distributing all ops in outgoing_qs once
    """
    while not all_outgoing_qs_empty(crdts):
        for curr_crdt in crdts:
            if not curr_crdt.outgoing_ops.empty():
                op = await curr_crdt.outgoing_ops.get() # I can't await this, because it will block if one site is idle
                # put in all other incoming_ops
                for other_crdt in list(filter(lambda other: other is not curr_crdt, crdts)):
                    await other_crdt.enqueue_op(op)
                curr_crdt.outgoing_ops.task_done()
                
def simulate_sync_nowait(crdts: [crdt.AbstractCRDT]):
    """
    Simulate synchronization, by distributing all ops in outgoing_qs once
    """
    while not all_outgoing_qs_empty(crdts):
        for curr_crdt in crdts:
            if not curr_crdt.outgoing_ops.empty():
                op = curr_crdt.outgoing_ops.get_nowait() 
                # put in all other incoming_ops
                for other_crdt in list(filter(lambda other: other is not curr_crdt, crdts)):
                    other_crdt.enqueue_op_nowait(op)
                curr_crdt.outgoing_ops.task_done()

def all_outgoing_qs_empty(crdts: [crdt.AbstractCRDT]) -> bool:
    for _crdt in crdts:
        if not _crdt.outgoing_ops.empty(): return False
    return True


def test_apply_local_sync_apply_remote(crdts: [crdt.AbstractCRDT], local_changes: asyncio.Future):

    loop = asyncio.get_event_loop()

    loop.run_until_complete(local_changes)
    for i in crdts: assert i.outgoing_ops.empty()
    print(f"""Local changes have been enqueued, incoming queues full""")

    apply_changes = asyncio.gather(*[i.process_queue() for i in crdts])

    loop.run_until_complete(apply_changes)
    for i in crdts: assert i.incoming_ops.empty()
    print(f"""Local changes have been applied, incoming queues empty, outgoing queues full""")

    sync_task = loop.create_task(simulate_sync(crdts))

    loop.run_until_complete(sync_task)
    for i in crdts: assert i.outgoing_ops.empty()
    print(f"""Local change have been propagated, incoming queues full, outgoing queues empty""")

    apply_changes = asyncio.gather(*[i.process_queue() for i in crdts])

    loop.run_until_complete(apply_changes)
    for i in crdts: assert i.outgoing_ops.empty() and i.outgoing_ops.empty()
    print(f"""Remote changes have been applied.""")

    assert len(set(crdts)) <= 1
    print("Consistent state")


# continous_changes_and_sync = asyncio.gather(crdt_test.make_n_random_local_changes_rfa(rga_1), 
#     crdt_test.make_n_random_local_changes_rfa(rga_2), 
#     simulate_network([rga_1, rga_2]),
#     rga_1.start_consuming(),
#     rga_2.start_consuming(),
#     )