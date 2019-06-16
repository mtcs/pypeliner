import pandas as pd

import pypeliner.parallel_pipeline as ppl


class PrintData(ppl.Persistence):
    def __init__(self, message):
        self.msg = message

    def persist(self, dataframe):
        print('\x1b[1;33m' + self.msg + '\x1b[0m')
        print(dataframe)


class ReadCSV(ppl.DataCreator):
    def __init__(self, file_path):
        self.file_path = file_path
        self.file = open(file_path, 'r')
        self.df_iterator = pd.read_csv(self.file_path, chunksize=3)

    def create(self, _):
        #print(type(self.df_iterator))
        try:
            df = self.df_iterator.get_chunk(size=self.partition_size)
        except StopIteration:
            return None

        return df


class CalculateColumn(ppl.Transformation):
    def __init__(self, col_name, src_col_name, func):
        self.col_name = col_name
        self.src_col_name = src_col_name
        self.func = func

    def transform(self, dataframe):

        if dataframe is None:
            return None
        # new_df = dataframe.copy()
        # new_df[self.col_name] = self.func(dataframe[self.src_col_name])
        # return new_df.drop(self.src_col_name, axis=1)
        dataframe[self.col_name] = dataframe[self.src_col_name].apply(self.func)
        return dataframe


def main():

    read_csv = ReadCSV('examples/data.csv')
    add_column_c = CalculateColumn('c', 'b', lambda v: abs(v))
    add_column_d = CalculateColumn('d', 'a', lambda v: -v)

    stages = [
        read_csv,
        add_column_c,
        add_column_d,
        PrintData('Parcial Output'),
    ]

    pipeline = ppl.ParallelPipeline(stages, partition_size=3)

    pipeline.run()


if __name__ == "__main__":
    main()
