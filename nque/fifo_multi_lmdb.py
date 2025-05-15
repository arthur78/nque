import logging
import lmdb

from nque import FifoBasicQueueLmdb
from nque.exc import ArgumentError, TryLater, QueueError

logger = logging.getLogger(__name__)


class FifoMultiQueueLmdb(FifoBasicQueueLmdb):
    """
    Adds support for multiple named internal queues.

    At least two internal queues are required to instantiate this class. (For
    a single queue use the parent FifoQueueLmdb class.)

    In the current design, queue producers write (put) to all internal
    queues at once in a single transaction (multicast). In opposite, queue
    consumers (get/remove/pop) have to stick using a particular internal queue.
    """

    # Maximum number of internal queues. The current value is experimental
    # and serves just as a guardrail.
    QUEUES_MAX = 10

    def __init__(
        self,
        db_path: str,
        queues: tuple[bytes, ...],
        use: bytes = None,
        items_count_max: int = 1_000,
        item_bytes_max: int = 20 * 1_024
    ) -> None:
        """
        :param db_path:
            Relative or absolute path of a created database file backing
            the queue.

        :param queues:
            Names of internal independent queues (LMDB namespaces). At least
            two queues is required.

        :param use:
            Name of internal queue to use. Required for queue consumers,
            and irrelevant in current design to queue producers, which write
            to all queues at once in a single transaction.

        :param items_count_max:
            Constraints the maximum number of items in each of the
            internal queues.

        :param item_bytes_max:
            Constraints the maximum number of bytes in a queue item.
        """
        self._validate_arg_queues(queues)
        self._validate_arg_use(use, queues)
        self._queues = queues
        self._use = use
        super().__init__(db_path, items_count_max, item_bytes_max)
        self._initialize_queues()

    def put(self, items: list[bytes] | tuple[bytes, ...]) -> None:
        if not self._use:
            super().put(items)
        else:
            raise QueueError(f"'put' cannot be used in consumer mode")

    def get(self, items_count: int = 1) -> list[bytes]:
        if self._use:
            return super().get(items_count)
        else:
            raise QueueError(f"'get' cannot be used in producer mode")

    def remove(self, items_count: int = 1) -> None:
        if self._use:
            super().remove(items_count)
        else:
            raise QueueError(f"'remove' cannot be used in producer mode")

    def pop(self, items_count: int = 1) -> list[bytes]:
        if self._use:
            return super().pop(items_count)
        else:
            raise QueueError(f"'pop' cannot be used in producer mode")

    def _initialize_queues(self):
        with self._env.begin(write=True) as txn:  # must be write-transaction
            for queue in self._queues:
                # Initialize internal queue by creating it
                self._env.open_db(queue, txn, create=True)

    def _get_env(self, db_path: str) -> lmdb.Environment:
        return lmdb.open(
            db_path,
            map_size=self._get_db_size(),
            max_dbs=len(self._queues))

    def _get_db_size(self) -> int:
        return len(self._queues) * super()._get_db_size()

    @staticmethod
    def _validate_arg_queues(queues: tuple[bytes, ...]) -> None:
        if len(queues) < 2:
            raise ArgumentError(f"required at least two internal queues")
        if len(queues) > FifoMultiQueueLmdb.QUEUES_MAX:
            raise ArgumentError(
                f"currently supported maximum number of internal queues "
                f"is {FifoMultiQueueLmdb.QUEUES_MAX}")
        if not all(isinstance(arg, bytes) for arg in queues):
            raise ArgumentError(f"all internal queues must be bytes literals")
        if not all(queues):
            raise ArgumentError(f"internal queues must be non-empty")
        if len(set(queues)) != len(queues):
            raise ArgumentError(f"internal queues must be unique")

    @staticmethod
    def _validate_arg_use(use: bytes, queues: tuple[bytes, ...]) -> None:
        if use is not None:
            if not isinstance(use, bytes):
                raise ArgumentError(f"'use' must be bytes literal")
            if not use:
                raise ArgumentError(f"'use' cannot be empty")
            if use not in queues:
                raise ArgumentError(f"'use' must be one of {queues}")

    def _put(self, items: list[bytes]) -> None:
        """
        Put given items to the end of each of the internal queues. Each item
        becomes a separate DB entry in each internal queue.

        DB keys for items are zero-filled strings created from unsigned
        integers, e.g. '001'.
        """
        try:
            with self._env.begin(write=True) as txn:
                for queue in self._queues:
                    named_db = self._env.open_db(queue, txn, create=False)
                    if self._permit_put(items, txn, named_db):
                        item_num = self._get_last_item_number(txn, named_db)
                        for item in items:
                            key = self._make_db_key(item_num)
                            txn.put(key, item, db=named_db)
                            item_num = self._get_next_item_number(item_num)
                        self._put_last_item_number(item_num, txn, named_db)
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

    def _get(self, items_count: int) -> list[bytes]:
        """
        Return N=items_count items from the beginning of the used internal
        queue w/o actual removing the returned items from the queue.

        This method should be used by a queue consumer, when it wants to
        remove the obtained items only in case it has successfully processed
        them.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        items = []
        try:
            with self._env.begin(write=False) as txn:
                named_db = self._env.open_db(self._use, txn, create=False)
                item_num = self._get_first_item_number(txn, named_db)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    item = txn.get(key, db=named_db)
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
        Remove N=items_count items from the beginning of the used named queue.

        Use this method only as complimentary to the 'get' method, after the
        queue consumer has successfully processed the items obtained with
        'get'.

        Note, however, that the items_count can differ from the one used
        previously for 'get', because it might have returned fewer items.

        For example:
            0 <= len(queue.get(100)) <= 100

        Thus, a proper removal is the consumer's responsibility.
        """
        try:
            with self._env.begin(write=True) as txn:
                named_db = self._env.open_db(self._use, txn, create=False)
                item_num = self._get_first_item_number(txn, named_db)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    if txn.get(key, db=named_db) is not None:
                        txn.delete(key, db=named_db)
                    else:
                        break
                    item_num = self._get_next_item_number(item_num)
                self._put_first_item_number(item_num, txn, named_db)
        except lmdb.Error as e:
            logger.error(f"failed to remove", exc_info=True)
            raise QueueError(e)

    def _pop(self, items_count: int) -> list[bytes]:
        """
        Pop N=items_count items from the beginning of the used internal queue.

        This is a get-remove operation done at once within a single
        transaction.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        items = []
        try:
            with self._env.begin(write=True) as txn:
                named_db = self._env.open_db(self._use, txn, create=False)
                item_num = self._get_first_item_number(txn, named_db)
                for i in range(items_count):
                    key = self._make_db_key(item_num)
                    item = txn.get(key, db=named_db)
                    if item is not None:
                        items.append(item)
                        txn.delete(key, db=named_db)
                        item_num = self._get_next_item_number(item_num)
                    else:
                        break
                self._put_first_item_number(item_num, txn, named_db)
                return items
        except lmdb.Error as e:
            logger.error(f"failed to pop", exc_info=True)
            raise QueueError(e)
