# Pypeliner
Organize your data workflow

Pypeliner is a simple framework to help organize data pypeline workflows. It makes easy to create, manage and visualize what is going on in a data engineering and data science application.

It can be used with major data science tools, like Pandas and PySpark.

## Simple example

```
import pypeliner_workflow.simple_pipeline as ppl


class SayFirst(ppl.Transformation):
    def transform(self, word_list):
        print(word_list[0], end=' ')
        return word_list[1]


class SaySecond(ppl.Transformation):
    def transform(self, last_word):
        print(last_word)


mypipeline = ppl.SimplePipeline([
    SayFirst(),
    SaySecond()
])

mypipeline.run(['hello', 'world'])
```

## More Complex Pipelines

You can run more complex pipelines with stages with alternate routes.

```
mypipeline = ppl.SimplePipeline([
    FirstStage(),
    SecondStage(some argument),
    [
        [ThirdStage(valueA)],  # Inputs from SecondStage
        [ThirdStage(valueB)]   # Inputs from SecondStage
    ],
    JoinStage() # Inputs a list containing output from both versions of ThirdStage
])
```

## Parallel Pipelines

Stages in a parallel pipeline run in a separate process. They input and output data to multipreocessing queues. Also, they read data in batches in order to fill the pipeline.

## Other examples
* (https://github.com/mtcs/pypeliner/blob/master/examples/pandas_example.py)[pandas_example.py]
* (https://github.com/mtcs/pypeliner/blob/master/examples/parallel_pandas_example.py)[parallel_pandas_example.py]
