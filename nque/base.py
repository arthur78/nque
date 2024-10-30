import abc

from nque.exc import ArgumentError


class PersistentQueue(abc.ABC):
    """A persistent queue that can be interacted with from different
    processes/threads.

    Abstract base class.
    """

    def __init__(self, items_count_max: int, item_bytes_max: int):
        """
        :param items_count_max:
            Maximum allowed number of items in the queue.

        :param item_bytes_max:
            Assumed maximum size of a queue item in bytes.
        """
        self._validate_arg_constraints(items_count_max, item_bytes_max)
        self.items_count_max = items_count_max
        self._item_bytes_max = item_bytes_max

    def put(self, items: list[bytes] | tuple[bytes, ...]) -> None:
        """Put the given items into the queue."""
        self._validate_arg_items(items)
        self._put(items)

    def get(self, items_count: int = 1) -> list[bytes]:
        """
        Return N=items_count items from the queue w/o removing.

        This method should be used by a queue consumer when it wants to
        remove the obtained items only in case it has successfully processed
        them.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        self._validate_arg_items_count(items_count)
        return self._get(items_count)

    def remove(self, items_count: int = 1) -> None:
        """Remove N=items_count items from the queue."""
        self._validate_arg_items_count(items_count)
        self._remove(items_count)

    def pop(self, items_count: int = 1) -> list[bytes]:
        """
        Pop N=items_count items from the queue.

        This is a get-remove operation done at once within a single
        transaction.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        self._validate_arg_items_count(items_count)
        return self._pop(items_count)

    @abc.abstractmethod
    def _put(self, items: list) -> None:
        pass

    @abc.abstractmethod
    def _get(self, items_count: int) -> list:
        pass

    @abc.abstractmethod
    def _remove(self, items_count: int) -> None:
        pass

    @abc.abstractmethod
    def _pop(self, items_count: int) -> list:
        pass

    @staticmethod
    def _validate_arg_constraints(items_count_max, item_bytes_max) -> None:
        if not isinstance(items_count_max, int):
            raise ArgumentError("items_count_max must be an integer")
        if items_count_max <= 0:
            raise ArgumentError("items_count_max must be positive")
        if not isinstance(item_bytes_max, int):
            raise ArgumentError("item_bytes_max must be an integer")
        if item_bytes_max <= 0:
            raise ArgumentError("item_bytes_max must be positive")

    def _validate_arg_items(self, items):
        if not isinstance(items, (list, tuple)):
            raise ArgumentError("items must be a list or tuple")
        if not items:
            raise ArgumentError("no items")
        if len(items) > self.items_count_max:
            raise ArgumentError(
                f"too many items [max: {self.items_count_max}]")
        if not all(isinstance(i, bytes) for i in items):
            raise ArgumentError("items must be bytes")
        if not all(len(i) <= self._item_bytes_max for i in items):
            raise ArgumentError(
                f"at least one item is too big [max allowed size: "
                f"{self._item_bytes_max} bytes]")

    def _validate_arg_items_count(self, items_count: int) -> None:
        if not isinstance(items_count, int):
            raise ArgumentError("items count must be an integer")
        if items_count <= 0:
            raise ArgumentError("items count must be > 0")
        if items_count > self.items_count_max:
            raise ArgumentError(
                f"items count must be <= {self.items_count_max}")


class FifoPersistentQueue(PersistentQueue, abc.ABC):
    """Base class for persistent FIFO queue."""
    pass
