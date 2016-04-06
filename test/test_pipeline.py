#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import time
from pipeline import Pipeline, PipeBuilder


class TestPipeline(unittest.TestCase):
    def test_one_pipe(self):
        pipeline = Pipeline().add(
            PipeBuilder().alias("multiplier").consumer(lambda m: m * m).buffer_size(100).number_of_consumer(10)
        )
        expected = [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

        actual = [x for x in pipeline.stream(range(10))]
        actual.sort()
        self.assertEquals(expected, actual)

    def test_multiple_pipes(self):
        pipeline = Pipeline().add(
            PipeBuilder("aggregator").aggregation_size(2).buffer_size(10)
        ).add(
            PipeBuilder("summation").consumer(lambda aggr: sum(aggr)).number_of_consumer(1).buffer_size(10)
        ).add(
            PipeBuilder("nth_triangular").consumer(lambda n: (n * n + n) / 2).number_of_consumer(1).buffer_size(10)
        )

        expect = [1, 15, 45, 91]
        self.assertEquals(expect, [x for x in pipeline.stream(range(8))])

    def test_when_consumer_yield_none(self):
        pipeline = Pipeline().add(
            PipeBuilder().alias("yield_none").consumer(lambda m: m if m == 0 else None).buffer_size(100).number_of_consumer(2)
        )
        expected = [0]
        actual = [x for x in pipeline.stream(range(100))]
        self.assertEquals(expected, actual)

    def test_an_exception_occurred_when_consuming_message_in_parallel(self):
        def test():
            pipeline = Pipeline()
            for x in pipeline.add(
                    PipeBuilder().alias("multiplier").consumer(lambda m: time.sleep(2) or m * m).buffer_size(100).number_of_consumer(2)
            ).stream(['a']):
                pass

        self.assertRaises(Exception, test)

    def test_run_multiple_times(self):
        pipeline = Pipeline().add(
            PipeBuilder("aggregator").aggregation_size(2).buffer_size(1)
        ).add(
            PipeBuilder("summation").consumer(lambda arr: sum(arr)).number_of_consumer(3).buffer_size(1)
        ).add(
            PipeBuilder("nth_triangular").consumer(lambda n: reduce(lambda n1, n2: n1 + n2, range(1, n), 1)).number_of_consumer(5).buffer_size(1)
        ).add(
            PipeBuilder("nth_triangular2").consumer(lambda n: reduce(lambda n1, n2: n1 + n2, range(1, n), 1)).number_of_consumer(5).buffer_size(
                1))

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

    def test_when_generator_makes_exception_pipeline_should_stop(self):
        pass

    def test_when_any_pipes_make_exception_pipeline_should_stop(self):
        pass


if __name__ == '__main__':
    unittest.main()
