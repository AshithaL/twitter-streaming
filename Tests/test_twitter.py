import unittest
import twitter
from mock import patch

class test_twitter(unittest.TestCase):

    ACCESS_TOKEN = 'some'
    ACCESS_SECRET = 'some'
    CONSUMER_KEY = 'some'
    CONSUMER_SECRET = 'some'

    def test_get_tweets(self):
        with patch('twitter') as test:
            test.connect().cursor().fetchall().return_value = ['twitter_analysis.db']
            X = bool(twitter.get_tweets())
            assert (X, "Success")

    def test_(self):
        http_res = "response"
        tcp_connection = "success"
        with patch('twitter') as test:
            test.connect().cursor().fetchall().return_value = ['twitter_analysis.db']
            X = bool(twitter.send_tweets_to_spark(http_res,tcp_connection))
            assert (X, "Success")


if __name__ == '__main__':
    unittest.main()
