#!

import pandas as pd

import pypeliner as ppl


class LoadData(ppl.Transformation):
    def transform(self, filename):
        return pd.read_csv(filename, index_col='id')


class CalculateAPlusB(ppl.Transformation):
    def transform(self, dataframe):
        dataframe['average'] = dataframe.mean(axis=1)
        return dataframe


class PrintData(ppl.Persistence):
    def persist(self, dataframe):
        print(dataframe)


class OutputCSV(ppl.Persistence):
    def __init__(self, output_filename):
        self. output_filename = output_filename

    def persist(self, dataframe):
        dataframe.to_csv(self.output_filename)


def main():
    # Configure Stages
    load_stage = LoadData()
    average_stage = CalculateAPlusB()
    print_data = PrintData()
    output_stage = OutputCSV('examples/data_out.csv')

    # Build pipeline
    pipeline = ppl.Pipeline([load_stage, print_data, average_stage, print_data, output_stage])

    # Run Pipeline
    pipeline.run('examples/data.csv')


if __name__ == "__main__":
    main()
