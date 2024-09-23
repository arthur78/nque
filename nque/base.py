import abc


class PersistentQueue(abc.ABC):
    """A persistent queue that can be interacted with from different
    processes/threads.

    Abstract base class.
    """

    @abc.abstractmethod
    def put(self, items: list) -> None:
        """Put the given items into the queue."""
        pass

    @abc.abstractmethod
    def get(self, items_count: int = 1) -> list:
        """
        Return N=items_count items from the queue w/o removing.

        This method should be used by a queue consumer, when it wants to
        remove the obtained items only in case it has successfully processed
        them.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        pass

    @abc.abstractmethod
    def remove(self, items_count: int = 1) -> None:
        """Remove N=items_count items from the queue."""

    @abc.abstractmethod
    def pop(self, items_count: int = 1) -> list:
        """
        Pop N=items_count items from the queue.

        This is a get-remove operation done at once within a single
        transaction.

        If the number of items in the queue is M, and M < N, then M items
        will be returned.
        """
        pass


class FifoPersistentQueue(PersistentQueue, abc.ABC):
    """A persistent FIFO queue base class."""
    pass
