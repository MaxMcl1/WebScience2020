#Task 1b

import tweepy
import json
import time

from datetime import datetime
from datetime import timedelta
from tweepy import StreamListener
from pymongo import MongoClient

consumer_key = 'ENTER CONSUMER KEY'
consumer_secret = 'ENTER SECRET CONSUMER KEY'
access_token = 'ENTER ACCESS TOKEN'
access_token_secret = 'ENTER ACCESS TOKEN SECRET'

client = MongoClient('localhost', 27017)
db = client['local']
collection = db['test_data']

LOCATIONS = [6.7499552751, 36.619987291, 18.4802470232, 47.1153931748, #Italy
            -7.57216793459, 49.959999905, 1.68153079591, 58.6350001085, #United Kingdom
            5.98865807458, 47.3024876979, 15.0169958839, 54.983104153, #Germany
            -54.5247541978, 2.05338918702, 9.56001631027, 51.1485061713, #France
            ]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

RUN_TIME = 30 # minutes

start_time = datetime.now()
end_time = start_time + timedelta(minutes=RUN_TIME)

def convert_to_datetime(status):
    t = status._json
    t['created_at'] = datetime.strptime(t['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    t['created'] = t.pop('created_at', None)
    return t

class CustomStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        collection.insert(convert_to_datetime(status))
        return True
     

    def on_error(self, status):
        if status == 420:
            return False
        print (status)

listenerStream = CustomStreamListener()
streamer = tweepy.Stream(auth=auth, listener=listenerStream)

# 1b) Enhanced Crawler, filtering on TRENDS - Started 11:48 9/03/2020
TRENDS = ['#coronavirus', 'cases', 'spread', 'NHS']
streamer.filter(track= TRENDS, locations=LOCATIONS, languages=['en'], is_async=True)
#streamer.sample(languages=["en"], is_async = True)

#query = "Coronavirus"

while datetime.now() < end_time:
    time.sleep(30)

streamer.disconnect()

print("Start time: ", start_time)
print("End time: ", end_time)

#t_rest = api.search(q = query, lang=['en'], count = 50)

# #print(t_rest)

#tweets = t_rest['statuses']

#for i in tweets:
#    i['created_at'] = datetime.strptime(i['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
#    i['created'] = i.pop('created_at', None)
#    collection.insert(i)



