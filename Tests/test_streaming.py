import unittest
import streaming
from mock import patch

class test_streaming(unittest.TestCase):


    def test_get_sql_context(self):
        spark_context = 'some'
        with patch('streaming') as test:
            test.connect().cursor().fetchall().return_value = ['twitter_analysis.db']
            X = bool(streaming.get_sql_context_instance(spark_context))
            assert (X, "Success")

    def test_process_rdd(self):
        rdd = 'some'
        time = '12:12'
        with patch('streaming') as test:
            test.connect().cursor().fetchall().return_value = ['twitter_analysis.db']
            X = bool(streaming.process_rdd(time,rdd))
            assert (X, "Success")


if __name__ == '__main__':
    unittest.main()
