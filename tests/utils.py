import time

from nque import FifoPersistentQueue, FifoBasicQueueLmdb, FifoMultiQueueLmdb


# A helper for testing FifoBasicQueueLmdb in separate processes
def fifo_queue_lmdb_put(db_path: str, items_count: int) -> None:
    _produce(FifoBasicQueueLmdb(db_path), items_count)


# A helper for testing FifoMultiQueueLmdb in separate processes
def fifo_multi_queue_lmdb_put(
    db_path: str,
    items_count: int,
    *queues: str
) -> None:
    _produce(FifoMultiQueueLmdb(db_path, *queues), items_count)


def _produce(queue: FifoPersistentQueue, items_count: int) -> None:
    for _ in range(items_count):
        queue.put([b'item' + str(_).encode()])
        time.sleep(0.001)
