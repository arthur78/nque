import time

from nque import FifoQueueLmdb


# A helper for testing FifoQueueLmdb in separate processes
def lmdb_put(db_path: str, items_count: int):
    producer = FifoQueueLmdb(db_path)
    for _ in range(items_count):
        producer.put([b'item' + str(_).encode()])
        time.sleep(0.001)
