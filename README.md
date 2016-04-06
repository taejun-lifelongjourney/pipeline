# Pipeline
A yet another tiny python library for chaining unit of work(job) and supporting parallel execution with multiple processes or threads

# Usage
```python
  pipeline = Pipeline().add(
      PipeBuilder("aggregator").aggregation_size(2).buffer_size(1)
  ).add(
      PipeBuilder("summation").consumer(lambda arr: sum(arr)).number_of_consumer(3).buffer_size(1)
  ).add(
      PipeBuilder("nth_triangular").consumer(lambda n: reduce(lambda n1, n2: n1 + n2, range(1, n), 1)).number_of_consumer(5).buffer_size(1)
  ).add(
      PipeBuilder("nth_triangular2").consumer(lambda n: reduce(lambda n1, n2: n1 + n2, range(1, n), 1)).number_of_consumer(5).buffer_size(1))

  expect = [1, 56, 407, 667, 3082]
  actual_results = []

  def do_test(i):
      actual = [x for x in pipeline.stream(range(9))]
      actual.sort()
      actual_results.append(actual)
      self.assertEquals(actual, expect, "%s th test. actual(%s) != expected(%s)" % (i, actual, expect))
      pipeline.reset()
      pipeline.logger.info("%sth test done", i)

  map(do_test, range(100))

  self.assertEquals(actual_results, map(lambda ignore: expect, range(100)))
  self.assertTrue(True)
```

# Dependendies
