import time
from collections import defaultdict

class BatchManager:
    def __init__(self, batch_size=100, flush_interval=100):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.batches = defaultdict(list)
        self.last_flush_time = defaultdict(lambda: time.time())

    def add_message(self, topic, message):
        self.batches[topic].append(message)

    def ready_to_flush(self, topic):
        return (
            len(self.batches[topic]) >= self.batch_size or
            time.time() - self.last_flush_time[topic] >= self.flush_interval
        )

    def flush(self, topic):
        batch = self.batches[topic]
        self.batches[topic] = []
        self.last_flush_time[topic] = time.time()
        return batch
