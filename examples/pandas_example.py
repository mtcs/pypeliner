#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools as ft
import logging

import pandas as pd

import pypeliner as ppl


class LoadData(ppl.Transformation):
    def transform(self, filename):
        return pd.read_csv(filename, index_col='id')


class CalculateAverage(ppl.Transformation):
    def transform(self, dataframe):
        dataframe['average'] = dataframe.mean(axis=1)
        return dataframe


class CalculateColumn(ppl.Transformation):
    def __init__(self, col_name, src_col_name, func):
        self.col_name = col_name
        self.src_col_name = src_col_name
        self.func = func

    def transform(self, dataframe):
        new_df = dataframe.copy()
        new_df[self.col_name] = self.func(dataframe[self.src_col_name])
        return new_df.drop(self.src_col_name, axis=1)


class JoinDataframes(ppl.Transformation):
    def __init__(self, join_by_col):
        self.join_by_col = join_by_col

    def transform(self, dataframe_list):
        return ft.reduce(pd.DataFrame.join, dataframe_list)


class PrintData(ppl.Persistence):
    def __init__(self, message):
        self.msg = message

    def persist(self, dataframe):
        print('\x1b[1;33m' + self.msg + '\x1b[0m')
        print(dataframe)


class OutputCSV(ppl.Persistence):
    def __init__(self, output_filename):
        self. output_filename = output_filename

    def persist(self, dataframe):
        dataframe.to_csv(self.output_filename)


def main():
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    # Configure Stages
    load_stage = LoadData()
    average_stage = CalculateAverage()
    create_abs_of_a = CalculateColumn('c', 'a', pd.DataFrame.abs)
    create_abs_of_b = CalculateColumn('d', 'b', pd.DataFrame.abs)
    join_by_id = JoinDataframes('id')
    output_stage = OutputCSV('examples/data_out.csv')

    # Build pipeline
    stages = [load_stage,
              PrintData('Input Data'),  # Outputs to substages
              [[create_abs_of_a],
               [create_abs_of_b]],  # Output a list of outputs
              PrintData('Data Before Join'),
              join_by_id,  # Joins output from substages
              average_stage,
              PrintData('Output Data'),
              output_stage]
    pipeline = ppl.SimplePipeline(stages)

    # Run Pipeline
    pipeline.run('examples/data.csv')


if __name__ == "__main__":
    main()
