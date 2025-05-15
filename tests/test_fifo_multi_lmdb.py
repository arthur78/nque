import multiprocessing as mp
import os
import shutil
import threading
import time
import unittest

from nque.fifo_multi_lmdb import FifoMultiQueueLmdb
from nque.exc import ArgumentError, TryLater, QueueError

from tests.utils import fifo_multi_queue_lmdb_put

CURRENT_DIR = os.path.dirname(__file__)


class TestFifoMultiQueueLmdb(unittest.TestCase):

    DB_PATH = os.path.join(CURRENT_DIR, '.queues', 'test-fifo-multi-lmdb')

    def setUp(self):
        self.producer = FifoMultiQueueLmdb(self.DB_PATH, (b'q1', b'q2'))
        self.consumer_1 = FifoMultiQueueLmdb(
            self.DB_PATH, (b'q1', b'q2'), use=b'q1')
        self.consumer_2 = FifoMultiQueueLmdb(
            self.DB_PATH, (b'q1', b'q2'), use=b'q2')

    def tearDown(self):
        shutil.rmtree(self.DB_PATH)

    def test_initial_entries_count(self):
        self.assertEqual(2, self.producer._env.stat()['entries'])

    def test_invalid_init_args(self):
        with self.assertRaises(ArgumentError):
            FifoMultiQueueLmdb(self.DB_PATH, (b'q1',))
            FifoMultiQueueLmdb(self.DB_PATH, (b'q1',), use=b'q1')
            FifoMultiQueueLmdb(self.DB_PATH, use=b'q1')
            FifoMultiQueueLmdb(self.DB_PATH, (b'q1', b'q2'), use=b'q3')
            FifoMultiQueueLmdb(self.DB_PATH, (b'', b'q2'), use=b'q2')
            FifoMultiQueueLmdb(self.DB_PATH, (b'q1', b'q1'))
            FifoMultiQueueLmdb(self.DB_PATH, (b'q1', b'q1'), use=b'q1')
            FifoMultiQueueLmdb(self.DB_PATH, ('q1', 'q2'), use=b'q1')
            FifoMultiQueueLmdb(self.DB_PATH, ('q1', 'q2'), use='q1')
            FifoMultiQueueLmdb(self.DB_PATH, (1, 2))
            FifoMultiQueueLmdb(self.DB_PATH, use='q1')
            FifoMultiQueueLmdb(
                self.DB_PATH,
                tuple([str(i).encode() for i in range(11)]))

    def test_put_okay(self):
        # Arrange
        items = [b'item1', b'item2']

        # Act & assert
        self.assertIsNone(self.producer.put(items))

    def test_put_items_limit(self):
        # Arrange
        items = [b'i' for _ in range(self.producer.items_count_max)]

        # Act & assert
        self.assertIsNone(self.producer.put(items))

    def test_put_items_limit_exceeded(self):
        # Arrange
        items = [b'item' for _ in range(self.producer.items_count_max)]
        self.producer.put(items)

        # Act & assert
        self.assertRaises(TryLater, self.producer.put, [b'overflow'])

    def test_put_items_limit_exceeded_2(self):
        # Arrange
        for _ in range(self.producer.items_count_max):
            self.producer.put([b'item'])

        # Act & assert
        self.assertRaises(TryLater, self.producer.put, [b'overflow'])

    def test_put_items_limit_exceeded_3(self):
        # Arrange
        items = [b'item' for _ in range(self.producer.items_count_max - 1)]

        # Act
        self.producer.put(items)

        # Assert
        self.assertRaises(TryLater, self.producer.put, items)

    def test_put_concurrent_threads(self):
        """
        Important: A single queue object MUST be shared across threads.
        Otherwise, for example, if we create an individual queue object per
        thread, the write transactions will not be isolated from each
        other.
        """
        # Arrange
        items_count = 10
        threads_count = 5
        total_items_count = items_count * threads_count
        threads = []

        def put(producer: FifoMultiQueueLmdb):
            for _ in range(items_count):
                producer.put([b'item' + str(_).encode()])
                time.sleep(0.001)

        # Act
        for i in range(threads_count):
            t = threading.Thread(target=put, args=(self.producer,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        items_1 = self.consumer_1.pop(items_count=total_items_count + 1)
        items_2 = self.consumer_2.pop(items_count=total_items_count + 1)

        # Assert
        self.assertEqual(total_items_count, len(items_1))
        self.assertEqual(total_items_count, len(items_2))

    def test_put_concurrent_processes(self):
        # Arrange
        items_count = 10
        processes_count = 5
        total_items_count = items_count * processes_count
        processes = []

        # Act
        for i in range(processes_count):
            p = mp.Process(
                target=fifo_multi_queue_lmdb_put,
                args=(self.DB_PATH, items_count, (b'q1', b'q2')))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
        items_1 = self.consumer_1.pop(items_count=total_items_count + 1)
        items_2 = self.consumer_2.pop(items_count=total_items_count + 1)

        # Assert
        self.assertEqual(total_items_count, len(items_1))
        self.assertEqual(total_items_count, len(items_2))

    def test_get(self):
        # Arrange
        items = [b'item1', b'item2']
        self.producer.put(items)

        # Act & assert
        self.assertEqual(items[:1], self.consumer_1.get())
        self.assertEqual(items[:1], self.consumer_1.get(1))
        self.assertEqual(items[:1], self.consumer_2.get())
        self.assertEqual(items[:1], self.consumer_2.get(1))
        self.assertEqual(items, self.consumer_1.get(2))
        self.assertEqual(items, self.consumer_1.get(20))
        self.assertEqual(items, self.consumer_2.get(2))
        self.assertEqual(items, self.consumer_2.get(20))

    def test_remove_by_one(self):
        # Arrange
        self.producer.put([b'item1', b'item2', b'item3'])

        # Act & assert
        #  First consumer using q1
        self.assertIsNone(self.consumer_1.remove())
        self.assertEqual([b'item2'], self.consumer_1.get())
        self.assertIsNone(self.consumer_1.remove())
        self.assertEqual([b'item3'], self.consumer_1.get())
        self.assertIsNone(self.consumer_1.remove())
        self.assertEqual([], self.consumer_1.get())
        self.assertIsNone(self.consumer_1.remove())
        self.assertEqual([], self.consumer_1.get())
        #  Second consumer using q2
        self.assertIsNone(self.consumer_2.remove())
        self.assertEqual([b'item2'], self.consumer_2.get())
        self.assertIsNone(self.consumer_2.remove())
        self.assertEqual([b'item3'], self.consumer_2.get())
        self.assertIsNone(self.consumer_2.remove())
        self.assertEqual([], self.consumer_2.get())
        self.assertIsNone(self.consumer_2.remove())
        self.assertEqual([], self.consumer_2.get())

    def test_remove_bulk(self):
        # Arrange
        self.producer.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertIsNone(self.consumer_1.remove(items_count=10))
        self.assertEqual([], self.consumer_1.get())
        self.assertIsNone(self.consumer_2.remove(items_count=10))
        self.assertEqual([], self.consumer_2.get())

    def test_remove_bulk2(self):
        # Arrange
        self.producer.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertIsNone(self.consumer_1.remove(items_count=2))
        self.assertEqual([b'item3'], self.consumer_1.get())
        self.assertIsNone(self.consumer_1.remove())
        self.assertEqual([], self.consumer_1.get())
        self.assertIsNone(self.consumer_2.remove(items_count=2))
        self.assertEqual([b'item3'], self.consumer_2.get())
        self.assertIsNone(self.consumer_2.remove())
        self.assertEqual([], self.consumer_2.get())

    def test_pop(self):
        # Arrange
        self.producer.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertEqual([b'item1'], self.consumer_1.pop())
        self.assertEqual([b'item2', b'item3'], self.consumer_1.pop(10))
        self.assertEqual([], self.consumer_1.pop(10))
        self.assertEqual([b'item1'], self.consumer_2.pop())
        self.assertEqual([b'item2', b'item3'], self.consumer_2.pop(10))
        self.assertEqual([], self.consumer_2.pop(10))

    def test_put_pop_cycle(self):
        # Act & assert
        for i in range(2 * self.producer.items_count_max):
            item = str(i).encode()
            self.producer.put([item])
            self.assertEqual([item], self.consumer_1.pop())
            self.assertEqual([item], self.consumer_2.pop())

    def test_put_get_remove_cycle(self):
        # Act & assert
        for i in range(2 * self.producer.items_count_max):
            item = str(i).encode()
            self.producer.put([item])
            self.assertEqual([item], self.consumer_1.get())
            self.assertEqual([item], self.consumer_2.get())
            self.consumer_1.remove()
            self.consumer_2.remove()

    def test_producer_with_faster_get_remove_consumer(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_faster_consume():
            consume_items_count = len(put_batch) + 1
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.get(consume_items_count)
                self.consumer_1.remove(consume_items_count)
                self.consumer_2.get(consume_items_count)
                self.consumer_2.remove(consume_items_count)

        # Act & assert
        self.assertIsNone(produce_and_faster_consume())

    def test_producer_with_slower_get_remove_consumer(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_slower_consume():
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.get()
                self.consumer_1.remove()
                self.consumer_2.get()
                self.consumer_2.remove()

        # Act & assert
        self.assertRaises(TryLater, produce_and_slower_consume)

    def test_producer_with_slower_get_remove_consumer_2(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_slower_consume():
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.get(len(put_batch))  # fast enough
                self.consumer_1.remove(len(put_batch))
                self.consumer_2.get()  # too slow
                self.consumer_2.remove()

        # Act & assert
        self.assertRaises(TryLater, produce_and_slower_consume)

    def test_producer_with_slower_pop_consumer(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_slower_consume():
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.pop()
                self.consumer_2.pop()

        # Act & assert
        self.assertRaises(TryLater, produce_and_slower_consume)

    def test_producer_with_slower_pop_consumer_2(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_slower_consume():
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.pop(len(put_batch))  # fast enough
                self.consumer_2.pop()

        # Act & assert
        self.assertRaises(TryLater, produce_and_slower_consume)

    def test_producer_with_faster_pop_consumer(self):
        # Arrange
        put_batch = [b'item1', b'item2', b'item3']

        def produce_and_faster_consume():
            consume_items_count = len(put_batch) + 1
            for i in range(2 * self.producer.items_count_max):
                self.producer.put(put_batch)
                self.consumer_1.pop(consume_items_count)
                self.consumer_2.pop(consume_items_count)

        # Act & assert
        self.assertIsNone(produce_and_faster_consume())

    def test_producer_unsupported_operation(self):
        # Arrange
        self.producer.put([b'item'])

        # Act & assert
        self.assertRaises(QueueError, self.producer.get)
        self.assertRaises(QueueError, self.producer.remove)
        self.assertRaises(QueueError, self.producer.pop)

    def test_consumer_unsupported_operation(self):
        # Arrange
        items = [b'item1']

        # Act & assert
        self.assertRaises(QueueError, self.consumer_1.put, items)
        self.assertRaises(QueueError, self.consumer_2.put, items)
