import asyncio
import random
import crdt
import crdt_test

if __name__=="__main__":

    rht_1 = crdt.HashMap(0, 2, 0, "SITE 1")
    rht_2 = crdt.HashMap(0, 2, 1, "SITE 2")

    NUM_PUTS = lambda: random.choice(range(10,15))
    NUM_REMOVES = lambda: random.choice(range(5,10))

    for i in range(3):

        print(f"Starting iteration {i}")

        print("PUT TEST")
        local_puts = asyncio.gather( crdt_test.make_n_random_local_ops_rht(rht_1, crdt.OpType.PUT, NUM_PUTS()), 
                                     crdt_test.make_n_random_local_ops_rht(rht_2, crdt.OpType.PUT, NUM_PUTS()))
        crdt_test.test_apply_local_sync_apply_remote([rht_1, rht_2], local_puts)

        print("REMOVE TEST")
        local_removes = asyncio.gather( crdt_test.make_n_random_local_ops_rht(rht_1, crdt.OpType.REMOVE, NUM_REMOVES()), 
                                        crdt_test.make_n_random_local_ops_rht(rht_2, crdt.OpType.REMOVE, NUM_REMOVES()))
        crdt_test.test_apply_local_sync_apply_remote([rht_1, rht_2], local_removes)
