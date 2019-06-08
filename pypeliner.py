#!

import abc
import collections


class Pipeline(object):
    def __init__(self, stages):
        if not isinstance(stages, collections.Iterable):
            stages = [stages]

        # Check if stages inherit from allowed abstract classes
        for stage in stages:
            assert issubclass(stage.__class__, Transformation) or \
                issubclass(stage.__class__, Persistence)

        self.stages = stages

    def run(self, input_data):
        last_stage_was_persistence = False
        for stage in self.stages:
            if issubclass(stage.__class__, Persistence):
                last_stage_was_persistence = True
                stage.persist(input_data)
            else:
                last_stage_was_persistence = False
                input_data = stage.transform(input_data)

        if last_stage_was_persistence:
            return None
        else:
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
