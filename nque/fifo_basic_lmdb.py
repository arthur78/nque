import math
import logging

import lmdb

from nque.exc import TryLater, QueueError, ArgumentError
from nque.base import FifoPersistentQueue

logger = logging.getLogger(__name__)


class FifoBasicQueueLmdb(FifoPersistentQueue):
    """
    A persistent basic FIFO queue implemented using LMDB.

    Multiple producer processes can be running concurrently. (Read further.)

    Only one consumer process is allowed if the queue consumption is done
    using 'get/remove' methods. Running multiple consumer processes
    concurrently in such a case will most probably lead to the queue
    corruption and must be avoided.

    Multiple consumer processes can be running concurrently, but only in case
    they all consume the queue using the 'pop' method only. (The 'pop' method
    gets and removes items from the queue within a single transaction.)

    Both producer and consumer processes are the queue writers and
    therefore will in fact interact with the queue sequentially, as LMDB
    allows only one writer at a time.
    """

    # Keys to hold the numbers of first and last items
    _START_KEY = b'start'
    _END_KEY = b'end'

    def __init__(
        self,
        db_path: str,
        items_count_max: int = 1_000,
        item_bytes_max: int = 20 * 1_024
    ) -> None:
        super().__init__(items_count_max, item_bytes_max)
        self._validate_arg_db_path(db_path)
        self._env = self._get_env(db_path)
        # How many zeros to fill into the item's numeric DB key
        self._zfill = math.ceil(math.log10(self.items_count_max))
        # TODO Enforce single consumer if using get/remove - set a
        #  special key in the DB.

    @classmethod
    def _get_first_item_number(
        cls,
        txn: lmdb.Transaction,
        named_db: lmdb._Database = None  # noqa
    ) -> int:
        """Get the number of the first queue item."""
        return int(txn.get(cls._START_KEY, b'0', db=named_db).decode())

    @classmethod
    def _put_first_item_number(
        cls,
        item_num: int,
        txn: lmdb.Transaction,
        named_db: lmdb._Database = None  # noqa
    ) -> None:
        """Store the number of the first queue item."""
        txn.put(cls._START_KEY, str(item_num).encode(), db=named_db)

    @classmethod
    def _get_last_item_number(
        cls,
        txn: lmdb.Transaction,
        named_db: lmdb._Database = None  # noqa
    ) -> int:
        """Get the number of the last queue item."""
        return int(txn.get(cls._END_KEY, default=b'0', db=named_db).decode())

    @classmethod
    def _put_last_item_number(
        cls,
        item_num: int,
        txn: lmdb.Transaction,
        named_db: lmdb._Database = None  # noqa
    ) -> None:
        txn.put(cls._END_KEY, str(item_num).encode(), db=named_db)

    def _get_env(self, db_path: str) -> lmdb.Environment:
        return lmdb.open(db_path, map_size=self._get_db_size(), max_dbs=1)

    def _pop(self, items_count: int) -> list[bytes]:
        """
        Pop N=items_count items from the beginning of the queue.

        This is a get-remove operation done at once within a single
        transaction.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
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
        except lmdb.Error as e:
            logger.error(f"failed to pop", exc_info=True)
            raise QueueError(e)

    def _get(self, items_count: int) -> list[bytes]:
        """
        Return N=items_count items from the beginning of the queue
        w/o actual removing the returned items from the queue.

        This method should be used by a queue consumer, when it wants to
        remove the obtained items only in case it has successfully processed
        them.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        items = []
        try:
            with self._env.begin(write=False) as txn:
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
        except lmdb.Error as e:
            logger.error(f"failed to get", exc_info=True)
            raise QueueError(e)

    def _remove(self, items_count: int) -> None:
        """
        Remove N=items_count items from the beginning of the queue.

        Use this method only as complimentary to the 'get' method, after the
        queue consumer has successfully processed the items obtained with
        'get'.

        Note, however, that the items_count can differ from the one used
        previously for 'get', because it might have returned fewer items.

        For example,
            0 <= len(queue.get(100)) <= 100

        Thus, a proper removal is the consumer's responsibility.
        """
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
        except lmdb.Error as e:
            logger.error(f"failed to remove", exc_info=True)
            raise QueueError(e)

    def _permit_put(
        self,
        items: list[bytes],
        txn: lmdb.Transaction,
        named_db: lmdb._Database = None  # noqa
    ) -> bool:
        """Whether we can permit putting the given items.

        Must be executed within a transaction.

        It is assumed that the basic pre-validation of items (items count,
        etc.) was already executed upstream.
        """
        # Projected number in the queue for the first item of given items
        first_item_number = self._get_last_item_number(txn, named_db)
        key = self._make_db_key(first_item_number)
        if txn.get(key, db=named_db) is not None:
            return False  # already taken, must not overwrite
        items_count = len(items)
        if items_count > 1:
            # Projected number in the queue for the last item of given items
            last_item_number = first_item_number
            for i in range(items_count - 1):
                last_item_number = self._get_next_item_number(last_item_number)
            key = self._make_db_key(last_item_number)
            if txn.get(key, db=named_db) is not None:
                return False  # already taken, must not overwrite
        return True

    def _put(self, items: list[bytes]) -> None:
        """
        Put given items to the end of the queue. Each item becomes a
        separate DB entry.

        DB keys for items are zero-filled strings created from unsigned
        integers, e.g. '001'.
        """
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
        items_total_size_max = self.items_count_max * self._item_bytes_max
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
        if previous_item_num == self.items_count_max - 1:
            next_item_num = 0  # start a new cycle
        else:
            next_item_num = previous_item_num + 1
        return next_item_num

    @staticmethod
    def _validate_arg_db_path(db_path: str) -> None:
        if not isinstance(db_path, str):
            raise ArgumentError(f"db_path must be a string")
        if not db_path:
            raise ArgumentError("db_path cannot be empty")
        # TODO advanced validation needed? E.g. invalid chars, etc?
