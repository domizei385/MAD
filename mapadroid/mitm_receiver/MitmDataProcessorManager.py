import sys
import threading
import time
from multiprocessing import JoinableQueue

from typing import List

from mapadroid.db.DbWrapper import DbWrapper
from mapadroid.mitm_receiver import MITMReceiver
from mapadroid.mitm_receiver.MitmMapper import MitmMapper
from mapadroid.mitm_receiver.SerializedMitmDataProcessor import \
    SerializedMitmDataProcessor
from mapadroid.utils.logging import LoggerEnums, get_logger
from mapadroid.utils.questGen import QuestGen

logger = get_logger(LoggerEnums.mitm)


class MitmDataProcessorManager:
    def __init__(self, args, mitm_mapper: MitmMapper, db_wrapper: DbWrapper, quest_gen: QuestGen):
        self._worker_threads: List[SerializedMitmDataProcessor] = []
        self._args = args
        self._mitm_data_queue: JoinableQueue = JoinableQueue()
        self._mitm_mapper: MitmMapper = mitm_mapper
        self._db_wrapper: DbWrapper = db_wrapper
        self._queue_check_thread = None
        self._stop_queue_check_thread = False
        self._queue_has_backlog_since = 0
        self._mitm_receiver_process: MITMReceiver = None

        self._queue_check_thread = threading.Thread(target=self._queue_size_check, args=())
        self._queue_check_thread.daemon = True
        self._queue_check_thread.start()
        self._quest_gen = quest_gen

    def get_queue(self):
        return self._mitm_data_queue

    def get_queue_size(self):
        # for whatever reason, there's no actual implementation of qsize()
        # on MacOS. There are better solutions for this but c'mon, who is
        # running MAD on MacOS anyway?
        try:
            item_count = self._mitm_data_queue.qsize()
        except NotImplementedError:
            item_count = 0

        return item_count

    def _queue_size_check(self):
        while not self._stop_queue_check_thread:
            item_count = self.get_queue_size()
            if item_count > 100:
                logger.warning("MITM data processing workers are falling behind! Queue length: {}", item_count)
                if not self._queue_has_backlog_since:
                    logger.warning("Notifying processors about backlog")
                    self._queue_has_backlog_since = time.time()
                    self._notify_drain(0.01)
                elif time.time() - self._queue_has_backlog_since > 60 * 30:
                    # Exit MAD after 30min of queue backlog
                    sys.exit("Queue backlog")
                if item_count > 5000:
                    self._notify_drain(0.5)
                elif item_count > 500:
                    self._notify_drain(item_count * 0.0001)
            elif self._queue_has_backlog_since:
                logger.warning("Notifying processors that there is no longer a backlog")
                self._queue_has_backlog_since = 0
                self._notify_drain(0.0)

            time.sleep(5)

    def _notify_drain(self, queue_drain: float):
        logger.warning("Notifying processors to drain from queue (" + str(queue_drain * 100) + "%)")
        if self._mitm_receiver_process:
            self._mitm_receiver_process.set_queue_drain(queue_drain)
        for worker_thread in self._worker_threads:
            worker_thread.set_queue_drain(queue_drain)

    def launch_processors(self):
        for i in range(self._args.mitmreceiver_data_workers):
            data_processor: SerializedMitmDataProcessor = SerializedMitmDataProcessor(
                self._mitm_data_queue,
                self._args,
                self._mitm_mapper,
                self._db_wrapper,
                self._quest_gen,
                name="SerializedMitmDataProcessor-%s" % str(i))

            data_processor.start()
            self._worker_threads.append(data_processor)

    def shutdown(self):
        self._stop_queue_check_thread = True

        logger.info("Stopping {} MITM data processors", len(self._worker_threads))
        for worker_thread in self._worker_threads:
            worker_thread.terminate()
            worker_thread.join()
        logger.info("Stopped MITM data processors")

        if self._mitm_data_queue is not None:
            self._mitm_data_queue.close()

    def set_mitm_receiver(self, mitm_receiver_process):
        self._mitm_receiver_process = mitm_receiver_process
