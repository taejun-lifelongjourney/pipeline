import ctypes
from multiprocessing import Queue, Process, Value
import logging
import collections
from threading import Thread
import time
import traceback

from counter import AtomicCounter


def _get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.WARN)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('[%(levelname)-7s %(asctime)s %(processName)s#%(process)d] %(name)s - %(message)s', "%H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def create_process_with(process_alias=None, target_func=None, daemon=True, **kwargs):
    process = Process(name=process_alias, target=target_func, kwargs=kwargs)
    process.daemon = daemon
    return process


def open_pipe_func(target_pipe=None, pipeline_running_status=None):
    target_pipe.open(pipeline_running_status=pipeline_running_status)
    return


class Pipeline:
    END_OF_STREAM_SIGNAL = "!end_of_stream!"

    RUNNING_STATUS_STANDBY = 0
    RUNNING_STATUS_RUNNING = 1
    RUNNING_STATUS_FINISH = 2
    RUNNING_STATUS_INTERRUPTED = -999

    @staticmethod
    def is_end_of_stream(data):
        return data == Pipeline.END_OF_STREAM_SIGNAL

    def __init__(self, alias=None):
        self.logger = _get_logger("pipeline")
        self._alias = alias
        self._pipe_builders = []

        self._pipes = {}
        self._pipe_processes = []
        self._first_pipe = None
        self._last_pipe = None
        self._func_read_stream = (lambda: range(0))

        self._cleanups = []
        self._already_cleanup = Value(ctypes.c_bool, False)

        self._running_status = Value(ctypes.c_int, Pipeline.RUNNING_STATUS_STANDBY)
        self._interrupted_by_exception = False

        self._thread_watching_running_status = None
        self._thread_watching_remaining_processes = None
        self._process_read_stream_from_generator = None

    def reset(self):
        self._pipes = {}
        self._pipe_processes = []
        self._first_pipe = None
        self._last_pipe = None
        self._func_read_stream = (lambda: range(0))

        self._cleanups = []
        self._already_cleanup = Value(ctypes.c_bool, False)

        self._running_status = Value(ctypes.c_int, Pipeline.RUNNING_STATUS_STANDBY)
        self._interrupted_by_exception = False

        self._thread_watching_running_status = None
        self._thread_watching_remaining_processes = None
        self._process_read_stream_from_generator = None

    def add(self, builder):
        """
        :param builder:
        :return: Pipeline
        """
        self._pipe_builders.append(builder)
        return self

    def stream(self, generator=None):
        """
        start to stream data from generator into pipeline(composed pipes), yielding data passed through pipeline

        :param generator: Iterable or Generator implementation
        :return:
        """
        # check running status
        if self.running_status != Pipeline.RUNNING_STATUS_STANDBY:
            raise Exception("invalid running status. Call reset() before call this")

        # change running status
        def cleanup_func_reset_running_status():
            with self._running_status.get_lock():
                if self.running_status != Pipeline.RUNNING_STATUS_INTERRUPTED:
                    self.set_running_status_to_finish()

        self._add_cleanup_func("reset running status of pipeline",
                               cleanup_func_reset_running_status)
        self.set_running_status_to_running()

        # determine stream generator
        if isinstance(generator, DataGenerator):
            self._func_read_stream = generator.produce
        elif isinstance(generator, collections.Iterable):
            self._func_read_stream = (lambda: generator)
        else:
            raise Exception("generator should be either Producer or Iterable")

        # configure pipes and create processes for them
        self._configure_pipes()

        # create thread for watching running status
        def watch_running_status(pipeline=None):
            pipeline.logger.info("start thread watching running status...")
            while True:
                if pipeline.running_status == Pipeline.RUNNING_STATUS_INTERRUPTED:
                    pipeline.logger.error("got interrupted, stop pipeline")
                    pipeline._interrupted_by_exception = True
                    pipeline.stop_force()
                    pipeline.set_running_status_to_finish()
                    break
                elif pipeline.running_status == Pipeline.RUNNING_STATUS_FINISH:
                    break
                time.sleep(0.001)
            pipeline.logger.info("stop thread watching running status")

        self._thread_watching_running_status = Thread(
            name="running_status_watcher",
            target=watch_running_status,
            kwargs={"pipeline": self})
        self._thread_watching_running_status.daemon = True

        # create new process streaming data into pipeline
        self._process_read_stream_from_generator = create_process_with(
            process_alias="data_generator",
            target_func=lambda pipeline: pipeline.stream_from_generator(),
            pipeline=self)

        # start thread watching running status
        self._thread_watching_running_status.start()

        # start new processes for each all pipes
        self.logger.info("start Processes for pipes(%s)...", len(self._pipe_processes))
        map(lambda process: process.start(),
            reduce(lambda p_group1, p_group2: p_group1 + p_group2, self._pipe_processes, []))
        self._add_cleanup_func("terminate all the pipe processes",
                               lambda: map(lambda each_p: each_p.terminate(),
                                           reduce(lambda p1, p2: p1 + p2, self._pipe_processes, [])))

        # start process reading stream from generator
        self.logger.info("start process for streaming data into pipeline...")
        self._add_cleanup_func("terminate the process reading stream from data generator",
                               lambda: self._process_read_stream_from_generator.terminate())
        self._process_read_stream_from_generator.start()

        # yield data being passed through pipeline
        self.logger.info("start to yield data stream from pipeline...")
        while True:
            message = self._last_pipe.outbound.get()
            if Pipeline.is_end_of_stream(message):
                break
            yield message
        self.logger.info("finish to yield data stream, get to end of stream")

        # wait for drainage
        self.logger.info("wait for all pipes' processes to stop...")
        self._join_pipes()
        self.logger.info("all pipes' processes stopped")

        # cleanup
        self._cleanup()

        # wait
        self._thread_watching_running_status.join()

        # if interrupted
        if self._interrupted_by_exception:
            raise Exception("processing was interrupted by unexpected exception")

        self.logger.info("finished successfully")

    def _configure_pipes(self):
        if self._pipe_builders is None or len(self._pipe_builders) <= 0:
            raise Exception("There are no pipes to stream data")

        # chaining pipes
        pipes = []
        pipe_outbound = Queue()
        self._pipe_builders.reverse()
        for builder in self._pipe_builders:
            pipe = builder.build()
            pipe.outbound = pipe_outbound
            pipes.append(pipe)
            pipe_outbound = pipe.inbound

        self._pipe_builders.reverse()
        pipes.reverse()
        self._pipes = pipes

        # capture entry and terminal
        self._first_pipe = self._pipes[0]
        self._last_pipe = self._pipes[-1]

        processes = []
        for each_pipe in self._pipes:
            processes_for_pipe = map(lambda i: create_process_with(process_alias="process-%s-%s" % (each_pipe.alias, i),
                                                                   target_func=open_pipe_func,
                                                                   target_pipe=each_pipe,
                                                                   pipeline_running_status=self._running_status),
                                     range(each_pipe.number_of_consumer))
            processes.append(processes_for_pipe)
        self._pipe_processes = processes

    def stream_from_generator(self):
        try:
            map(lambda m: self.__stream_data(m), self._func_read_stream())
            self.__stream_data(Pipeline.END_OF_STREAM_SIGNAL)
        except Exception as e:
            self.logger.info("when read stream from generator, an unexpected exception occurred, stopping pipeline. "
                             "see cause -> %s", e)
            traceback.print_exc()
            self.set_running_status_to_interrupted()

    def __stream_data(self, data):
        self._first_pipe.inbound.put(data)

    def _join_pipes(self):
        def wait_processes_to_finish(pipeline=None, processes=None):
            pipeline.logger.info("start thread watching pipe processes remaining...")
            while True:
                processes_alive = filter(lambda p: p.is_alive(), reduce(lambda plist1, plist2: plist1 + plist2, processes, []))
                if len(processes_alive) <= 0:
                    pipeline.logger.info("no remaining processes")
                    break
                else:
                    pipeline.logger.info("%s remaining processes : %s", len(processes_alive),
                                         map(lambda p: (p.pid, p.name), processes_alive))
                time.sleep(0.001)
            pipeline.logger.info("stop thread watching pipe processes remaining")

        self._thread_watching_remaining_processes = Thread(
            name="remaining_processes_watcher",
            target=wait_processes_to_finish,
            kwargs={"pipeline": self,
                    "processes": self._pipe_processes}
        )

        self._thread_watching_remaining_processes.daemon = True
        self._thread_watching_remaining_processes.start()

        map(lambda p:
            self.logger.debug("joining(waiting) the process(name:%s, id:%s, alive:%s)...", p.name, p.pid, p.is_alive())
            or p.join()
            or self.logger.debug("released joining the process(name:%s, id:%s, alive:%s)", p.name, p.pid, p.is_alive()),
            reduce(lambda plist1, plist2: plist1 + plist2, self._pipe_processes, []))

        self._thread_watching_remaining_processes.join()

    def _add_cleanup_func(self, desc="", func=(lambda: None)):
        """
        :rtype : object
        """
        self._cleanups.append((desc, func))

    def _cleanup(self):
        with self._already_cleanup.get_lock():
            if self._already_cleanup.value:
                return

            self.logger.info("start clean up...")
            map(lambda cleanup_tuple:
                self.logger.info("call cleanup func -> %s", cleanup_tuple[0])
                or cleanup_tuple[1](),
                self._cleanups)
            self.logger.info("finish clean up")
            self._already_cleanup.value = True

    def stop_force(self):
        """
        terminate all spawned processes
        :return: void
        """
        # call registered cleanups
        self._cleanup()

        # send end signal to terminal queue for pipeline
        self._last_pipe.outbound.put(Pipeline.END_OF_STREAM_SIGNAL)

    @property
    def running_status(self):
        return self._running_status.value

    def set_running_status_to_standby(self):
        self._set_running_status(Pipeline.RUNNING_STATUS_STANDBY)

    def set_running_status_to_running(self):
        self._set_running_status(Pipeline.RUNNING_STATUS_RUNNING)

    def set_running_status_to_finish(self):
        self._set_running_status(Pipeline.RUNNING_STATUS_FINISH)

    def set_running_status_to_interrupted(self):
        self._set_running_status(Pipeline.RUNNING_STATUS_INTERRUPTED)

    def _set_running_status(self, value):
        with self._running_status.get_lock():
            self._running_status.value = value


class Pipe(object):
    def __init__(self, alias=None,
                 consumer=None,
                 buffer_size=0,
                 number_of_consumer=1,
                 skip_on_error=False,
                 inbound_counter=AtomicCounter(),
                 outbound_counter=AtomicCounter(),
                 consumer_exception_handler=None, **kwargs):
        self._alias = alias
        self._logger = _get_logger("pipe-%s" % self._alias)
        self._buffer_size = buffer_size
        self._consumer = consumer
        self._number_of_consumer = number_of_consumer
        self._active_consumer_counter = AtomicCounter()
        self._skip_on_error = skip_on_error
        self._inbound_counter = inbound_counter
        self._outbound_counter = outbound_counter
        self._inbound = Queue(self._buffer_size)
        self._outbound = None
        self._consumer_exception_handler = consumer_exception_handler
        self._additional_properties = kwargs

    def open(self, pipeline_running_status=None):
        with self._active_consumer_counter.lock:
            self._active_consumer_counter.increase()
            self._info("open consumer, %s of %s consumer(s)", self._active_consumer_counter.value,
                       self._number_of_consumer)
        try:
            map(
                lambda message: self._downstream(message),
                self._poll_and_consume_and_yield(self._read_one_from_stream)
            )
        except Exception as e:
            self._handle_exception(exception=e, pipeline_running_status=pipeline_running_status)

        with self._active_consumer_counter.lock:
            self._active_consumer_counter.decrease()
            self._info("close consumer, %s consumer(s) remaining", self._active_consumer_counter.value)

    def _read_one_from_stream(self):
        message = self._inbound.get()
        self._info("<< input %s", message)

        if Pipeline.is_end_of_stream(message):
            with self._active_consumer_counter.lock:
                if self._active_consumer_counter.value > 1:
                    # re-product end of stream signal for other sibling pipe processes
                    self._inbound.put(message)
        else:
            self._inbound_counter.increase()
        return message

    def _downstream(self, message=None):
        if not Pipeline.is_end_of_stream(message):
            self._outbound_counter.increase()

        if self._outbound is None:
            return

        if Pipeline.is_end_of_stream(message):
            # if and only if current pipe process is the last remaining, send end-of-stream signal downstream.
            with self._active_consumer_counter.lock:
                if self._active_consumer_counter.value <= 1:
                    self._outbound.put(message)
                    self._info(">> output - %s", message)
        else:
            self._outbound.put(message)
            self._info(">> output - %s", message)

    def _poll_and_consume_and_yield(self, func_read_from_upstream):
        return []

    def _handle_consumer_exception(self, consumer_exception, message):
        if self._consumer_exception_handler is None:
            return False
        try:
            self._consumer_exception_handler(consumer_exception, message)
            return True
        except Exception as e:
            self._warn("failed to invoke a consumer exception handler with a consumer exception. see cause -> %s", e.message)
            return False

    def _handle_exception(self, exception=None, pipeline_running_status=None):
        with pipeline_running_status.get_lock():
            if pipeline_running_status.value == Pipeline.RUNNING_STATUS_INTERRUPTED:
                return
            else:
                pipeline_running_status.value = Pipeline.RUNNING_STATUS_INTERRUPTED
                self._error("when processing data stream on pipeline, an unexpected exception has occurred. "
                            "This will cause the pipeline to stop. see cause -> %s\n%s",
                            exception,
                            traceback.format_exc())

    def _debug(self, message, *args, **kwargs):
        self._log(logging.DEBUG, message, *args, **kwargs)

    def _info(self, message, *args, **kwargs):
        self._log(logging.INFO, message, *args, **kwargs)

    def _warn(self, message, *args, **kwargs):
        self._log(logging.WARNING, message, *args, **kwargs)

    def _error(self, message, *args, **kwargs):
        self._log(logging.ERROR, message, *args, **kwargs)

    def _log(self, level, message, *args, **kwargs):
        self._logger.log(level, message, *args, **kwargs)

    @property
    def alias(self):
        return self._alias

    @property
    def inbound(self):
        return self._inbound

    @property
    def outbound(self):
        return self._outbound

    @outbound.setter
    def outbound(self, outbound):
        self._outbound = outbound

    @property
    def number_of_consumer(self):
        return self._number_of_consumer

    @property
    def skip_on_error(self):
        return self._skip_on_error

    @property
    def additional_properties(self):
        return self._additional_properties

    def inbound_count(self):
        return self._inbound_counter.value

    def outbound_count(self):
        return self._outbound_counter.value


class DefaultPipe(Pipe):
    def __init__(self,
                 alias=None,
                 consumer=None,
                 number_of_consumer=1,
                 counter=AtomicCounter(),
                 skip_on_error=False,
                 buffer_size=0,
                 consumer_exception_handler=None,
                 aggregation_size=1
                 ):
        super(DefaultPipe, self).__init__(alias=alias, consumer=consumer, number_of_consumer=number_of_consumer,
                                          counter=counter, skip_on_error=skip_on_error, buffer_size=buffer_size,
                                          consumer_exception_handler=consumer_exception_handler)
        self._aggregation_size = aggregation_size
        self._aggregation_buffer = []
        self._aggregation_count = 0

    def _poll_and_consume_and_yield(self, read_one_from_stream):
        while True:
            message = read_one_from_stream()

            # check end of stream
            if Pipeline.is_end_of_stream(message):
                # flush aggregation buffer
                if self._aggregation_size > 1 and self._aggregation_count >= 1:
                    yield self._aggregation_buffer
                    self._aggregation_count = 0
                    self._aggregation_buffer = []

                # stream end of stream signal downstream also
                yield message
                break

            # delegate message to consumer
            processed_message = message
            if self._consumer is not None:
                try:
                    processed_message = self._consumer.consume(message) if isinstance(self._consumer, Consumer) else self._consumer(message)
                    self._info("processed %s to %s", message, processed_message)
                except Exception as e:
                    handled = self._handle_consumer_exception(e, message)
                    if self._skip_on_error:
                        if not handled:
                            self._warn("failed to consume a message(%s). see cause -> %s ", message, e)
                    else:
                        raise ConsumerException(message="failed to consume message",
                                                cause=e,
                                                data=message,
                                                stacktrace=traceback.format_exc())

            if processed_message is None:
                continue

            # emit downstream
            if self._aggregation_size <= 1:
                yield processed_message
                continue

            self._aggregation_count += 1
            self._aggregation_buffer.append(processed_message)
            if self._aggregation_count >= self._aggregation_size:
                yield self._aggregation_buffer
                self._aggregation_count = 0
                self._aggregation_buffer = []


class Consumer(object):
    def __init__(self, alias):
        self._alias = alias

    def consume(self, message):
        pass


class DataGenerator(object):
    def __init__(self):
        pass

    def produce(self):
        """
        have to yield each data to stream into pipeline
        :return: any type of data
        """
        pass


class PipeBuilder(object):
    PIPE_TYPE = 'pipe'
    ALIAS = 'alias'
    CONSUMER = "consumer"
    NUMBER_OF_CONSUMER = "number_of_consumer"
    BUFFER_SIZE = "buffer_size"
    SKIP_ON_ERROR = "skip_on_error"
    INBOUND_COUNTER = "inbound_counter"
    OUTBOUND_COUNTER = "outbound_counter"
    AGGREGATION_SIZE = "aggregation_size"
    CONSUMER_EXCEPTION_HANDLER = "consumer_exception_handler"

    def __init__(self, alias=None, pipe_type=DefaultPipe):
        self._properties = {}
        self.alias(alias)
        self.pipe_type(pipe_type)

    def pipe_type(self, pipe_type):
        self.set(PipeBuilder.PIPE_TYPE, pipe_type)
        return self

    def alias(self, alias):
        self.set(PipeBuilder.ALIAS, alias)
        return self

    def consumer(self, consumer):
        self.set(PipeBuilder.CONSUMER, consumer)
        return self

    def number_of_consumer(self, number_of_consumer):
        self.set(PipeBuilder.NUMBER_OF_CONSUMER, number_of_consumer)
        return self

    def buffer_size(self, skip_on_error):
        self.set(PipeBuilder.BUFFER_SIZE, skip_on_error)
        return self

    def inbound_counter(self, counter):
        self.set(PipeBuilder.INBOUND_COUNTER, counter)
        return self

    def outbound_counter(self, counter):
        self.set(PipeBuilder.OUTBOUND_COUNTER, counter)
        return self

    def aggregation_size(self, aggregation_size):
        self.set(PipeBuilder.AGGREGATION_SIZE, aggregation_size)
        return self

    def consumer_exception_handler(self, consumer_exception_handler):
        self.set(PipeBuilder.CONSUMER_EXCEPTION_HANDLER, consumer_exception_handler)
        return self

    def set(self, attr, value):
        self._properties[attr] = value

    def get(self, attr):
        return self._properties[attr]

    def exists(self, attr):
        return attr in self._properties

    def build(self):
        pipe_kwargs = dict(filter(lambda item: item[0] != PipeBuilder.PIPE_TYPE, self._properties.items()))
        return self._properties[PipeBuilder.PIPE_TYPE](**pipe_kwargs)


class ConsumerException(Exception):
    def __init__(self,
                 message=None,
                 cause=None,
                 data=None,
                 stacktrace=None):
        self.message = message
        self.cause = cause
        self.data = data
        self.stacktrace = data
        super(ConsumerException, self).__init__(message, cause, data, stacktrace)
