import shutil
import unittest

from nque.exc import ArgumentError, QueueError, TryLater
from nque.fifo import FifoQueueLmdb


class TestFifoQueueLmdb(unittest.TestCase):

    DB_PATH = '.db/test-db'

    def setUp(self):
        self.queue = FifoQueueLmdb(self.DB_PATH)

    def tearDown(self):
        shutil.rmtree(self.DB_PATH)

    def test_initial_entries_count(self):
        self.assertEqual(0, self.queue._env.stat()['entries'])

    def test_put_invalid_arg(self):
        # Act & assert
        self.assertRaises(ArgumentError, self.queue.put, 1)
        self.assertRaises(ArgumentError, self.queue.put, "item")
        self.assertRaises(ArgumentError, self.queue.put, None)
        self.assertRaises(ArgumentError, self.queue.put, [])
        self.assertRaises(ArgumentError, self.queue.put, (0, ))
        self.assertRaises(ArgumentError, self.queue.put, [1, 2])
        self.assertRaises(ArgumentError, self.queue.put, {})
        self.assertRaises(ArgumentError, self.queue.put, True)
        self.assertRaises(ArgumentError, self.queue.put, b"item")
        too_big_item = ((FifoQueueLmdb.ITEM_MAX + 1) * '.').encode()
        self.assertRaises(ArgumentError, self.queue.put, [too_big_item])
        too_many_items = [b'item' for _ in range(FifoQueueLmdb.ITEMS_MAX + 1)]
        self.assertRaises(ArgumentError, self.queue.put, too_many_items)
        self.assertRaises(ArgumentError, self.queue.put, [b'item', 'item'])

    def test_put_okay(self):
        # Act & assert
        self.assertIsNone(self.queue.put([b'item1', b'item2']))
        self.assertIsNone(self.queue.put([b'item3']))
        self.assertIsNone(self.queue.put((b'item4', )))

    def test_put_items_limit(self):
        # Arrange
        items = [b'item' for i in range(FifoQueueLmdb.ITEMS_MAX)]

        # Act & assert
        self.assertIsNone(self.queue.put(items))

    def test_put_items_limit_exceeded(self):
        # Arrange & act
        for i in range(FifoQueueLmdb.ITEMS_MAX):
            self.queue.put([str(i).encode()])

        # Assert
        self.assertRaises(TryLater, self.queue.put, [b'overflow'])

    def test_get(self):
        # Arrange
        self.queue.put([b'item1', b'item2'])

        # Act & assert
        self.assertEqual([b'item1'], self.queue.get())
        self.assertEqual([b'item1'], self.queue.get(1))
        self.assertEqual([b'item1', b'item2'], self.queue.get(2))
        self.assertEqual([b'item1', b'item2'], self.queue.get(20))

    def test_remove_by_one(self):
        # Arrange
        self.queue.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertIsNone(self.queue.remove())
        self.assertEqual([b'item2'], self.queue.get())
        self.assertIsNone(self.queue.remove())
        self.assertEqual([b'item3'], self.queue.get())
        self.assertIsNone(self.queue.remove())
        self.assertEqual([], self.queue.get())
        self.assertIsNone(self.queue.remove())
        self.assertEqual([], self.queue.get())

    def test_remove_bulk(self):
        # Arrange
        self.queue.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertIsNone(self.queue.remove(items_count=10))
        self.assertEqual([], self.queue.get())

    def test_pop(self):
        # Arrange
        self.queue.put([b'item1', b'item2', b'item3'])

        # Act & assert
        self.assertEqual([b'item1'], self.queue.pop())
        self.assertEqual([b'item2', b'item3'], self.queue.pop(10))
        self.assertEqual([], self.queue.pop(10))

    def test_put_pop_cycle(self):
        # Arrange
        producer = self.queue
        consumer = FifoQueueLmdb(self.DB_PATH)

        # Act & assert
        for i in range(2 * FifoQueueLmdb.ITEMS_MAX):
            item = str(i).encode()
            producer.put([item])
            self.assertEqual([item], consumer.pop())

    def test_put_get_remove_cycle(self):
        # Arrange
        producer = self.queue
        consumer = FifoQueueLmdb(self.DB_PATH)

        # Act & assert
        for i in range(2 * FifoQueueLmdb.ITEMS_MAX):
            item = str(i).encode()
            producer.put([item])
            self.assertEqual([item], consumer.get())
            consumer.remove()
