import abc
import collections
import logging
import multiprocessing as mp
import time

import base_pipeline
from base_pipeline import BasePipeline, BaseStage


WAIT_TIME = 0.1
MAX_QUEUE_SIZE = 2

LOG = logging.getLogger(__name__)


def enable_dashboard():
    base_pipeline.enable_pipeline()


def control_queue_size(queue):
    # Control Queue size
    while len(queue) >= MAX_QUEUE_SIZE:
        time.sleep(WAIT_TIME)


def const_input_handler_process(stage, stage_func, input_stream, output_stream):
    input_data = input_stream.get()

    while True:

        out = stage_func(input_data)
        if out is not None:
            LOG.warning(f'Executed {stage.__class__.__name__} step')

        output_stream.put(out)

        if out is None or \
                (isinstance(out, list) and not out):
            break

        # control_queue_size(output_stream)

    output_stream.put(None)
    LOG.warning(f'Exiting {stage.__class__.__name__} process')


def part_input_handler_process(stage, stage_func, input_stream, output_stream):
    while True:
        input_data = input_stream.get()
        if input_data is None or\
                (isinstance(input_data, list) and input_data) == []:
            break

        out = stage_func(input_data)
        if output_stream is not None:
            output_stream.put(out)

        LOG.warning(f'Executing {stage.__class__.__name__} step')

        # control_queue_size(output_stream)

    if output_stream is not None:
        output_stream.put(None)
    LOG.warning(f'Exiting {stage.__class__.__name__} process')


def wide_input_handler_process(stage, stage_func, input_stream, output_stream):
    flat_input_data = []
    while True:
        input_data = input_stream.get()

        if input_data is None or\
                (isinstance(input_data, list) and input_data) == []:
            break

        flat_input_data.extend(input_data)

    if flat_input_data:
        LOG.warning(f'Executing {stage.__class__.__name__} step')
        out = stage_func(flat_input_data)
    else:
        out = []

    for partition in out:
        if output_stream is not None:
            output_stream.put(out)

        # control_queue_size(output_stream)

    if output_stream is not None:
        output_stream.put(None)
    LOG.warning(f'Exiting {stage.__class__.__name__} process')


class ParallelPipeline(BasePipeline):
    """ A out of core parallel pipeline class. It reads data in lbocks and lauches a process to
    execute each stage.
    """

    processes = []

    def __init__(self, stages, partition_size=None):
        super().__init__()
        self.partition_size = partition_size
        if not isinstance(stages, collections.Iterable):
            stages = [stages]

        self.stages = stages

    def __del__(self):
        for process in self.processes:
            process.terminate()

    def _start_stage_processes(self, input_stream):
        output_stream = None

        # TODO Solve queue concurrent get
        for stage in self.stages:
            if self.partition_size and isinstance(stage, Stage):
                stage.partition_size = self.partition_size

            if issubclass(stage.__class__, Transformation):
                # Transformation: input and output
                output_stream = mp.Queue(maxsize=MAX_QUEUE_SIZE)
                new_proc = mp.Process(
                    target=part_input_handler_process,
                    args=(stage, stage.transform, input_stream, output_stream))
                input_stream = output_stream

            elif issubclass(stage.__class__, WideTransformation):
                # Transformation: input and output
                output_stream = mp.Queue(maxsize=MAX_QUEUE_SIZE)
                new_proc = mp.Process(
                    target=wide_input_handler_process,
                    args=(stage, stage.transform, input_stream, output_stream))
                input_stream = output_stream

            elif issubclass(stage.__class__, DataCreator):
                # DataCreator: no input and a output
                output_stream = mp.Queue(maxsize=MAX_QUEUE_SIZE)
                new_proc = mp.Process(
                    target=const_input_handler_process,
                    args=(stage, stage.create, input_stream, output_stream))
                input_stream = output_stream

            elif issubclass(stage.__class__, Persistence):
                # Persistence: input and no output
                new_proc = mp.Process(
                    target=part_input_handler_process,
                    args=(stage, stage.persist, input_stream, None))

            elif issubclass(stage.__class__, Stage):
                LOG.error('Propper stage inheritance not identified on class'
                          f' {stage.__class__.__name__}! Inheriting Stage class is not enough!')
                exit()

            else:
                LOG.error('Propper stage inheritance not identified on class'
                          f' {stage.__class__.__name__}!')
                exit()

            new_proc.start()
            self.processes.append(new_proc)

    def set_partition_size(self, partition_size):
        self.partition_size = partition_size

    def run(self, input_data=None):
        input_stream = mp.Queue()
        input_stream.put(input_data)
        self._start_stage_processes(input_stream)

        for process in self.processes:
            process.join()


class Stage(BaseStage):
    partition_size = None


class DataCreator(Stage):
    @abc.abstractmethod
    def create():
        return []


class Transformation(Stage):
    @abc.abstractmethod
    def transform(self, input_data):
        return input_data


class WideTransformation(Stage):
    @abc.abstractmethod
    def transform(self, input_data):
        return input_data


class Persistence(Stage):
    """ A Persistence class is a Stage class that persists data in a n > None cardinality. It
    has a persist function which receives data and usually outputs somthing else, like a file or
    console output.
    """
    @abc.abstractmethod
    def persist(self, input_data):
        """
        :input_data: input data to be persisted
        """
        pass


class PartitionnedFileReader(DataCreator):
    def __init__(self, file_path):
        self.file = open(file_path, 'r')

    def __del__(self):
        self.file.close()

    def create(self, _):
        # lines = []
        # line_count = 0
        # while(self.file):
        #     lines.append(self.file.readline())
        #     if line_count >= self.block_size:
        #         return lines

        return self.file.readlines(self.block_size)



