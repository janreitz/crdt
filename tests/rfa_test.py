import random
import asyncio
import functools

import crdt
import crdt_test

rfa_1 = crdt.FixedSizeArray(0,2,0,10, "SITE 1")
rfa_2 = crdt.FixedSizeArray(0,2,1,10, "SITE 2")

NUM_CHANGES = 10

continous_changes_and_sync = asyncio.gather(crdt_test.make_continuous_local_changes_rfa(rfa_1, .1), 
    crdt_test.make_continuous_local_changes_rfa(rfa_2, .1), 
    crdt_test.simulate_network([rfa_1, rfa_2]),
    rfa_1.start_consuming(),
    rfa_2.start_consuming(),
    crdt_test.print_states_rfa([rfa_1, rfa_2], 1)
    )

loop = asyncio.get_event_loop()

loop.run_forever()

# for i in range(10):

#     print(f"Starting iteration {i}")

#     local_changes = asyncio.gather(crdt_test.make_n_random_local_changes_rfa(rfa_1, NUM_CHANGES), 
#         crdt_test.make_n_random_local_changes_rfa(rfa_2, NUM_CHANGES), 
#         )

#     loop.run_until_complete(local_changes)
#     assert rfa_1.incoming_ops.qsize() == NUM_CHANGES and rfa_1.outgoing_ops.empty()
#     assert rfa_2.incoming_ops.qsize() == NUM_CHANGES and rfa_2.outgoing_ops.empty()
#     print(f"""Local changes have been enqueued, incoming queues full""")
#     # {rfa_1},
#     # {rfa_2}
#     # """)

#     apply_changes = asyncio.gather(rfa_1.process_queue(),
#         rfa_2.process_queue(),
#         )

#     loop.run_until_complete(apply_changes)
#     assert rfa_1.incoming_ops.empty() and rfa_1.outgoing_ops.qsize() == NUM_CHANGES
#     assert rfa_2.incoming_ops.empty() and rfa_2.outgoing_ops.qsize() == NUM_CHANGES
#     print(f"""Local changes have been applied, incoming queues empty, outgoing queues full""")
#     # {rfa_1},
#     # {rfa_2}
#     # """)

#     sync_task = loop.create_task(crdt_test.simulate_sync([rfa_1, rfa_2]))

#     loop.run_until_complete(sync_task)
#     assert rfa_1.incoming_ops.qsize() == NUM_CHANGES and rfa_1.outgoing_ops.empty()
#     assert rfa_2.incoming_ops.qsize() == NUM_CHANGES and rfa_2.outgoing_ops.empty()
#     print(f"""Local change have been propagated, incoming queues full, outgoing queues empty""")
#     # {rfa_1},
#     # {rfa_2}
#     # """)

#     apply_changes = asyncio.gather(rfa_1.process_queue(),
#         rfa_2.process_queue(),
#         )
#     loop.run_until_complete(apply_changes)
#     assert rfa_1.incoming_ops.empty() and rfa_1.outgoing_ops.empty()
#     assert rfa_2.incoming_ops.empty() and rfa_2.outgoing_ops.empty()
#     print(f"""Remote changes have been applied.""")
#     # {rfa_1},
#     # {rfa_2}
#     # """)
#     assert rfa_1.array == rfa_2.array
#     print("Consistent state")