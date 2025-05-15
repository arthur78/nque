# nque - Persistent FIFO Queue Implementation in Python
![Version](https://img.shields.io/badge/version-1.0.0-blue)
![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![Coverage](https://img.shields.io/badge/coverage-92%25-blue)
![License](https://img.shields.io/badge/license-MIT-blue)


`nque` is a Python library that implements persistent FIFO queues backed by [LMDB](https://www.symas.com/mdb) (Lightning
Memory-Mapped Database). Suitable for building data pipelines and handling data exchange between producer and consumer 
processes. The queues operate in memory for performance while persisting data to disk for reliability. For detailed API 
information and usage examples, please refer to the class and method docstrings.

## Features

- Transaction-safe operations
- Two queue implementations:
  - Basic FIFO queue (`FifoBasicQueueLmdb`) for single queue scenarios
  - Multi-queue system (`FifoMultiQueueLmdb`) supporting multiple named queues with broadcast capability
- Configurable queue size and item size limits

## Requirements

- Python 3.12+
- LMDB

## Installation

```bash
pip install nque
```

## Usage Examples

### Basic FIFO Queue

```python
from nque import FifoBasicQueueLmdb

# Initialize queue with max 1000 items of 20KB each
queue = FifoBasicQueueLmdb(
    db_path="queue", 
    items_count_max=1000,
    item_bytes_max=20 * 1024
)

# Add items
queue.put([b"item1", b"item2"])

# Get items without removing (peek)
items = queue.get(2)

# Remove items after successful processing 
queue.remove(2)

# Get and remove items atomically
items = queue.pop(2)
```

### Multi-Queue System

```python
from nque import FifoMultiQueueLmdb

# Create producer that broadcasts to all internal named queues
producer = FifoMultiQueueLmdb(
    db_path="multiqueue",
    queues=(b"queue1", b"queue2")
)

# Create consumer for specific internal queue
consumer = FifoMultiQueueLmdb(
    db_path="multiqueue",
    queues=(b"queue1", b"queue2"),
    use=b"queue1"
)

# Broadcast to all queues atomically
producer.put([b"broadcast_message"])

# Consume from specific queue
items = consumer.pop(1)
```

## Error Handling

The library provides three exception types:

- `QueueError`: Base exception for general queue operations
- `ArgumentError`: Invalid argument errors 
- `TryLater`: Temporary condition preventing operation completion

## Performance Considerations

- Queue max size is pre-defined using max items and item size
- Write operations (both producers and consumers) are serialized through LMDB's internal locking

## API Reference

### Common Queue Parameters

- `db_path`: Path to the LMDB database storing the queue data
- `items_count_max`: Maximum number of items in the queue (default: 1000)
- `item_bytes_max`: Maximum size of each item in bytes (default: 20KB)

### Multi-Queue Additional Parameters

- `queues`: Names of internal queues producers broadcast to
- `use`: Name of queue to consume from (for consumers only)

### Common Methods

- `put(items)`: Add items to the queue
- `get(items_count)`: Retrieve items without removing
- `remove(items_count)`: Remove items from the queue
- `pop(items_count)`: Atomic get and remove operation

## Best Practices and Error Prevention

1. Use `get()` to retrieve items first, then call `remove()` after successful processing
2. Use `pop()` for atomic operations, particularly when concurrent consumers are needed
3. Implement retry logic for `TryLater` exceptions

## Limitations

- Write operations (including from consumers) are serialized
- Get/remove operation pattern requires single consumer per queue
- Multi-queue supports maximum of 10 internal queues
- LMDB constraints apply to overall performance

## Future Development

The `nque` library is designed with extensibility in mind. Future development areas may include:

- Support for monitoring and metrics capabilities
- Additional queue patterns and implementations
- Support for additional backend storage systems beyond LMDB

Suggestions and contributions are welcome.

## Author

Artur Khakimau

## License

[MIT](https://opensource.org/licenses/MIT)
