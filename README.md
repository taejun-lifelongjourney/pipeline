# Pipeline
A yet another tiny python library for pipelining sequantial data processing steps and running them in parallel way with multiple processes

# Usage
```python

#initialize pipeline with 3 pipes
pipeline = Pipeline().add(
    PipeBuilder("aggregator").aggregation_size(2).buffer_size(1)
).add(
    PipeBuilder("summation").consumer(lambda arr: sum(arr)).number_of_consumer(3).buffer_size(1)
).add(
    PipeBuilder("triangular").consumer(lambda n: (n * n + n) / 2).number_of_consumer(5).buffer_size(1)
)

#pours data into pipelilne using generator
[1, 15, 45, 91] == [for x in pipeline.stream(range(8))]

"""
Explanation
range(8) streams [0,1,2,3,4,6,7] into the pipeline
aggerator streams [[0,1], [2,3], [4,5], [6,7]] into the next pipe(summation)
summation streams 1, 5, 9, 13 into the next pipe(nth_triangular)
triangular streams out 1, 15, 45, 91 for each input
"""
```  

You can also throttle streams(data) using buffer_size of each pipe's consumer and number_of_consumer.

Data generator for pipeline can be one of below :

* any built-in DataGenerator class implementors
* collections.Iterables
* any user-defined generator function

Also each consumer can be one of below :
* any lamda function that receive one argument
* any built-in Consumer class implementors
