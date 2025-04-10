import logging
import json
import datetime
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

class AsyncJsonLogger:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.log_queue = Queue(-1)

        # Create the final output handler (write to file)
        self.file_handler = logging.FileHandler(log_file_path)
        self.file_handler.setFormatter(logging.Formatter('%(message)s'))

        # Queue listener runs in the background
        self.listener = QueueListener(self.log_queue, self.file_handler)
        self.listener.start()

        # Queue handler for asynchronous writes
        self.queue_handler = QueueHandler(self.log_queue)
        self.logger = logging.getLogger("etl_async_logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.queue_handler)
        self.logger.propagate = False

    def log(self, dag_id, task_id, step, status, message, **extra):
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "dag_id": dag_id,
            "task_id": task_id,
            "step": step,
            "status": status,
            "message": message,
            "execution_date": str(datetime.date.today()),
            "extra": extra
        }
        self.logger.info(json.dumps(log_entry))

    def stop(self):
        self.listener.stop()


if __name__ == '__main__':
    log_file = f"C:\\Users\\003G1M744\\Personal\\Training Material\\async logging\\logs\\log.jsonl"
    logger = AsyncJsonLogger(log_file)
    logger.log('dag_id', 'task_id', "start", "in_progress", "Starting data extraction")
    logger.stop()