#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import collections
import logging

_LOG = logging.getLogger(__name__)


# Recursive stage list solver
def _solve_stage_list(input_data, stages):
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
            stage.persist(input_data)

        else:
            # Transform Transformation stages
            input_data = stage.transform(input_data)

    return input_data


class SimplePipeline(object):
    def __init__(self, stages):
        if not isinstance(stages, collections.Iterable):
            stages = [stages]

        # Check if stages inherit from allowed abstract classes
        for stage in stages:
            assert issubclass(stage.__class__, Transformation) or \
                issubclass(stage.__class__, Persistence) or \
                isinstance(stage, list)

        self.stages = stages

    def run(self, input_data):
        _solve_stage_list(input_data, self.stages)

        return input_data


class Transformation(object):
    def __init__(self):
        pass

    @abc.abstractmethod
    def transform(self, input_data):
        return


class Persistence(object):
    def __init__(self):
        pass

    @abc.abstractmethod
    def persist(self, input_data):
        pass
