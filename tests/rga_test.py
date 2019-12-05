import asyncio
import random
import crdt
import crdt_test

if __name__=="__main__":

    rga_1 = crdt.RGALinkedList(0, 2, 0, "SITE 1")
    rga_2 = crdt.RGALinkedList(0, 2, 1, "SITE 2")

    NUM_CHANGES = lambda: random.choice(range(5,15))

    for i in range(3):

        print(f"Starting iteration {i}")

        print("INSERTION TEST")
        local_inserts = asyncio.gather( crdt_test.make_n_random_local_ops_rga(rga_1, crdt.OpType.INSERT, NUM_CHANGES()), 
                                        crdt_test.make_n_random_local_ops_rga(rga_2, crdt.OpType.INSERT, NUM_CHANGES()))
        crdt_test.test_apply_local_sync_apply_remote([rga_1, rga_2], local_inserts)

        print("DELETE TEST")
        local_deletes = asyncio.gather( crdt_test.make_n_random_local_ops_rga(rga_1, crdt.OpType.DELETE, NUM_CHANGES()), 
                                        crdt_test.make_n_random_local_ops_rga(rga_2, crdt.OpType.DELETE, NUM_CHANGES()))
        crdt_test.test_apply_local_sync_apply_remote([rga_1, rga_2], local_deletes)

        print("UPDATE TEST")
        local_updates = asyncio.gather( crdt_test.make_n_random_local_ops_rga(rga_1, crdt.OpType.UPDATE, NUM_CHANGES()), 
                                        crdt_test.make_n_random_local_ops_rga(rga_2, crdt.OpType.UPDATE, NUM_CHANGES()))
        crdt_test.test_apply_local_sync_apply_remote([rga_1, rga_2], local_updates)

