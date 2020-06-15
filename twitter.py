import socket
import sys
import requests
import requests_oauthlib
import json
from sql_connection import conn

# Replace the values below with yours
ACCESS_TOKEN = '1252513694992330753-YpQY1SlyBWIN66ngHXeM8hcZWvvTeZ'
ACCESS_SECRET = 'reoC4xZdgp3bqRPjTC2ptxn00vUPrftWlhprHOBIp29jA'
CONSUMER_KEY = 'eLsiPuE8adtsJUt8hr0iMku3b'
CONSUMER_SECRET = 'p03sqgt8V8TYZbueGzA3SQPZXI5xuhpU5DkPj4fOGyra8YTiXn'
auth_handler = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'),
                  ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=auth_handler, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for lines in http_resp.iter_lines():
        try:
            full_tweet = json.loads(lines)
            words = full_tweet['text'].split(' ')
            tweet = ''
            for w in words:
                if '#' in w:
                    i = "".join(w.split(' '))
                    tweet += i
                    break
            time = full_tweet['created_at']
            location = "".join(full_tweet["user"]["location"].encode("utf-8"))
            if tweet is not '':
                tweet_text = tweet.encode('utf-8') + '&%' + location + '&%' + time
                print("Tweet Text: " + tweet_text)
                tcp_connection.send(tweet_text + '\n')
                conn.execute(
                    'INSERT INTO tweet (time, tweet, location) VALUES (%s,%s,%s,%s,%s)',
                    (str(time), tweet, str(location)))
                conn.commit()

        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)