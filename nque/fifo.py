import abc
import math
import logging

import lmdb

from nque.exc import TryLater, ArgumentError, QueueError
from nque.base import PersistentQueue

logger = logging.getLogger(__name__)


class FifoQueue(PersistentQueue, abc.ABC):
    """A persistent FIFO queue base class."""
    pass


class FifoQueueLmdb(FifoQueue):
    """
    A persistent FIFO queue implemented using LMDB.

    Multiple producer processes can be running concurrently. (Read further.)

    Only one consumer process is allowed if the queue consumption is done
    using 'get/remove' methods. Running multiple consumer processes
    concurrently in such the case will most probably lead to the queue
    corruption and must be avoided.

    Multiple consumer processes can be running concurrently, but only in case
    they all consume the queue using the 'pop' method only. (The 'pop' method
    gets and removes items from the queue within a single transaction.)

    Both producer and consumer processes are the queue writers and
    therefore will in fact interact with the queue sequentially, as LMDB
    allows only one writer at a time.
    """

    # Assumed maximum size of a queue item in bytes
    ITEM_MAX = 20 * 1024

    # Set the maximum number of allowed items in a queue
    ITEMS_MAX = 1_000

    # Keys to hold the numbers of first and last items
    _START_KEY = b'start'
    _END_KEY = b'end'

    def __init__(self, db_path: str) -> None:
        self._env = lmdb.open(db_path, map_size=self._get_db_size(), max_dbs=1)
        # How many zeros to fill into the item's numeric DB key
        self._zfill = math.ceil(math.log10(self.ITEMS_MAX))
        # TODO Enforce single consumer if using get/remove - set a
        #  special key in the DB.

    def put(self, items: list[bytes] | tuple[bytes, ...]) -> None:
        """
        Put given items to the end of the queue. Each item becomes a
        separate DB entry.

        DB keys for items are zero-filled strings created from unsigned
        integers, e.g. '001'.
        """
        if not isinstance(items, (list, tuple)):
            raise ArgumentError("items must be a list or tuple")
        if not items:
            raise ArgumentError("no items")
        if len(items) > self.ITEMS_MAX:
            raise ArgumentError(f"too many items [max: {self.ITEMS_MAX}]")
        if not all(isinstance(i, bytes) for i in items):
            raise ArgumentError("items must be bytes")
        if not all(len(i) <= self.ITEM_MAX for i in items):
            raise ArgumentError(f"too big item [max: {self.ITEM_MAX} bytes]")
        return self._put(items)

    def get(self, items_count: int = 1) -> list[bytes]:
        """
        Return N=items_count items from the beginning of the queue
        w/o actual removing the returned items from the queue.

        This method should be used by a queue consumer, when it wants to
        remove the obtained items only in case it has successfully processed
        them.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        self._validate_arg_items_count(items_count)
        return self._get(items_count)

    def remove(self, items_count: int = 1) -> None:
        """Remove N=items_count items from the beginning of the queue.

        Use this method only as complimentary to the 'get' method, after the
        queue consumer has successfully processed the items obtained with
        'get'.

        Note, however, that the items_count can differ from the one used
        previously for 'get', because it might have returned fewer items.

        For example:
            0 <= len(queue.get(100)) <= 100

        Thus, a proper removal is the consumer's responsibility.
        """
        self._validate_arg_items_count(items_count)
        self._remove(items_count)

    def pop(self, items_count: int = 1) -> list[bytes]:
        """
        Pop N=items_count items from the beginning of the queue.

        This is a get-remove operation done at once within a single
        transaction.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        self._validate_arg_items_count(items_count)
        return self._pop(items_count)

    @classmethod
    def _get_first_item_number(cls, txn: lmdb.Transaction) -> int:
        """Get the number of the first queue item."""
        return int(txn.get(cls._START_KEY, b'0').decode())

    @classmethod
    def _put_first_item_number(
        cls,
        item_num: int,
        txn: lmdb.Transaction
    ) -> None:
        """Store the number of the first queue item."""
        txn.put(cls._START_KEY, str(item_num).encode())

    @classmethod
    def _get_last_item_number(cls, txn: lmdb.Transaction) -> int:
        """Get the number of the last queue item."""
        return int(txn.get(cls._END_KEY, b'0').decode())

    @classmethod
    def _put_last_item_number(
        cls,
        item_num: int,
        txn: lmdb.Transaction
    ) -> None:
        txn.put(cls._END_KEY, str(item_num).encode())

    def _validate_arg_items_count(self, items_count: int) -> None:
        if not isinstance(items_count, int):
            raise ArgumentError("items count must be an integer")
        if items_count <= 0:
            raise ArgumentError("items count must be > 0")
        if items_count > self.ITEMS_MAX:
            raise ArgumentError(f"items count must be <= {self.ITEMS_MAX}")

    def _pop(self, items_count: int) -> list[bytes]:
        items = []
        try:
            with self._env.begin(write=True) as txn:
                item_num = self._get_first_item_number(txn)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    item = txn.get(key)
                    if item is not None:
                        items.append(item)
                        txn.delete(key)
                        item_num = self._get_next_item_number(item_num)
                    else:
                        break
                self._put_first_item_number(item_num, txn)
                return items
        except lmdb.Error:
            logger.error(f"failed to pop", exc_info=True)

    def _get(self, items_count: int) -> list[bytes]:
        items = []
        try:
            with self._env.begin(write=True) as txn:
                item_num = self._get_first_item_number(txn)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    item = txn.get(key)
                    if item is not None:
                        items.append(item)
                        item_num = self._get_next_item_number(item_num)
                    else:
                        break
                return items
        except lmdb.Error:
            logger.error(f"failed to get", exc_info=True)

    def _remove(self, items_count: int) -> None:
        try:
            with self._env.begin(write=True) as txn:
                item_num = self._get_first_item_number(txn)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    if txn.get(key) is not None:
                        txn.delete(key)
                    else:
                        break
                    item_num = self._get_next_item_number(item_num)
                self._put_first_item_number(item_num, txn)
        except lmdb.Error:
            logger.error(f"failed to remove", exc_info=True)

    def _permit_put(self, items: list[bytes], txn: lmdb.Transaction) -> bool:
        """Whether we can permit putting the given items.

        Must be executed within a transaction.
        """
        special_keys_count = 0
        if txn.get(self._START_KEY) is not None:
            special_keys_count += 1
        if txn.get(self._END_KEY) is not None:
            special_keys_count += 1
        current_items_count = self._env.stat()['entries'] - special_keys_count
        future_items_count = len(items) + current_items_count
        return future_items_count <= self.ITEMS_MAX

    def _put(self, items: list[bytes]) -> None:
        try:
            with self._env.begin(write=True) as txn:
                if self._permit_put(items, txn):
                    item_num = self._get_last_item_number(txn)
                    for item in items:
                        key = self._make_db_key(item_num)
                        txn.put(key, item)
                        item_num = self._get_next_item_number(item_num)
                    self._put_last_item_number(item_num, txn)
                else:
                    raise TryLater
        except TryLater:
            logger.warning(f"put not permitted: try later")
            # Producers might want to try to repeat in a short while,
            # as by that time consumers might free-up some space.
            raise
        except lmdb.Error as e:
            logger.error(f"failed to put", exc_info=True)
            raise QueueError(e)

    def _get_db_size(self) -> int:
        """
        We have to make sure the 'put' operation will not lead us to
            "lmdb.MapFullError: Environment mapsize limit reached"
        exception.

        If the db size limit is reached, the "write" transactions cannot be
        started anymore, which means that not only the queue producers but
        also the queue consumers will not be able to read, as they are also
        the queue writers.
        """
        items_total_size_max = self.ITEMS_MAX * self.ITEM_MAX
        # Double total allowed items size to ensure we not reach DB maxsize.
        # If we do not allocate more space, then there's a good chance we
        # reach the DB max size before we reach the items max count.
        # Reaching the DB max size will not let run the queue consumers,
        # as they are also the queue writers.
        return 2 * items_total_size_max

    def _make_db_key(self, key: int) -> bytes:
        return str(key).zfill(self._zfill).encode()

    def _get_next_item_number(self, previous_item_num: int) -> int:
        """Return the number of the next item in the queue."""
        if previous_item_num == self.ITEMS_MAX - 1:
            next_item_num = 0  # start a new cycle
        else:
            next_item_num = previous_item_num + 1
        return next_item_num
