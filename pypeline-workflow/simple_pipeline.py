#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import collections
import logging

_LOG = logging.getLogger(__name__)


# Recursive stage list solver
def _solve_stage_list(input_data, stages):
    """ Solves a pipeline stage list
    :input_data: input data
    :stages: list of stages or list of subpipelines
    """
    for stage in stages:
        if isinstance(stage, list):
            # Recursivelly call list of stages
            _LOG.debug(f'Junction found with {len(stage)} subpipelines')
            out_data = list(range(len(stage)))
            for i, substagelist in enumerate(stage):
                assert isinstance(substagelist, list)
                out_data[i] = _solve_stage_list(input_data, substagelist)

            input_data = out_data

        elif issubclass(stage.__class__, Persistence):
            # Persist Persistance stages
            stage._persist(input_data)

        else:
            # Transform Transformation stages
            input_data = stage._transform(input_data)

    return input_data


class SimplePipeline(object):
    """ A SimplePipeline is a syncronous serial pipeline. Every stage is executed exclusivelly until it outputs data or returns completely.
    """
    def __init__(self, stages):
        """
        :stages: a list of stages objects or subpipelines decribing the pipeline
        """
        if not isinstance(stages, collections.Iterable):
            stages = [stages]

        # Check if stages inherit from allowed abstract classes
        for stage in stages:
            assert issubclass(stage.__class__, Stage) or \
                isinstance(stage, list)

        self.stages = stages

    def run(self, input_data):
        """
        :input_data: data the will be the input of the forst stage/subpipelines
        :returns: The output of tha last Transformation stage
        """
        _solve_stage_list(input_data, self.stages)

        return input_data


class Stage(object):
    """ A Stage is a basic component of a pipeline. When execute by the pipeline, it can either
    transform or persist data.
    """
    def _run(self):
        _LOG.info(f'Runnung {self.__class__.__name__}')


class Transformation(Stage):
    """ A Transcformation class is a Stage class that transforms data in a n > n cardinality. It
    has a tranform function which receives data and outputs data.
    """
    def __init__(self):
        """
        """
        pass

    def _transform(self, input_data):
        """
        :input_data: input data to be transformed
        """
        self._run()
        return self.transform(input_data)

    @abc.abstractmethod
    def transform(self, input_data):
        """ A tranformation function that receives data and outputs data
        :input_data: input data to be transformed
        :returns :
        """
        return input_data


class Persistence(Stage):
    """ A Persistence class is a Stage class that persists data in a n > None cardinality. It
    has a persist function which receives data and usually outputs somthing else, like a file or
    console output.
    """
    def __init__(self):
        """
        """
        pass

    def _persist(self, input_data):
        """
        :input_data: input data to be persisted
        """
        self._run()
        self.persist(input_data)

    @abc.abstractmethod
    def persist(self, input_data):
        """
        :input_data: input data to be persisted
        """
        pass
