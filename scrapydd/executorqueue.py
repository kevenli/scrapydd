from Queue import Queue
from threading import Lock
import logging

class ExecutorQueue():
    internal_queues = {}
    internal_queues_lock = Lock()


    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def append(self,spider_id):
        self.internal_queues_lock.acquire()
        if not spider_id in self.internal_queues:
            spider_queue = Queue()
            self.internal_queues[spider_id] = spider_queue
        else:
            spider_queue = self.internal_queues[spider_id]
        self.internal_queues_lock.release()

        if spider_queue.not_empty:
            self.logger.warn('Queue contains item, ignore new execution')
            return

        spider_queue.put(spider_id)
