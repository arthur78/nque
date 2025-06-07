class TryLater(Exception):
    """Indicates that a queue operation cannot be completed at the moment."""
    pass


class QueueError(Exception):
    """General purpose queue error."""
    pass


class ArgumentError(QueueError):
    """Indicates wrong argument for a queue method."""
    pass
