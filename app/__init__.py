import queue

SMS_MAX_QUEUE_SIZE = 50000
SMS_QUEUE: queue.Queue = queue.Queue(maxsize=SMS_MAX_QUEUE_SIZE)
