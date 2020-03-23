import tweepy
import json
import time

from datetime import datetime
from tweepy import StreamListener
from pymongo import MongoClient

consumer_key = 'Q8TMdaLORBXACDyUiPPEjTrwh'
consumer_secret = 'rUIISVRh6Or1mFTXs5B5tJzqblIbC95PAtGVBJCkFRSLrpIZ2h'
access_token = '556272598-dsbAS16lkO67WzBVPjpryQEU3TZOSJ85Sp6h7DCF'
access_token_secret = 'jA3hILG1srQvPP62aHwovj0Ce0DIoFOtB03CTuPMMNtEx'

client = MongoClient('localhost', 27017)
db = client['local']
collection = db['Crawler 1a']

LOCATIONS = [6.7499552751, 36.619987291, 18.4802470232, 47.1153931748, #Italy
            -7.57216793459, 49.959999905, 1.68153079591, 58.6350001085, #United Kingdom
            5.98865807458, 47.3024876979, 15.0169958839, 54.983104153, #Germany
            -54.5247541978, 2.05338918702, 9.56001631027, 51.1485061713, #France
            ]

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        #print("Here")

    def on_status(self, status):
        t = json.loads(status)
        dt = t['created_at']
        dt = datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
        collection.insert(t)

    def on_data(self, data):
        t = json.loads(data)
        #dt = t['created_at']
        #dt = datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
        collection.insert(t)

    def on_error(self, status):
        print (status)
        return True

    def on_timeout(self):
        print("Timeout")
        return True

listenerStream = CustomStreamListener()
streamer = tweepy.Stream(auth=auth, listener=listenerStream)

# 1a) Simple crawler for collecting 1% data - Started 11:39 9/03/2020 
#streamer.filter()
#streamer.sample()

# 1b) Enhanced Crawler, filtering on TRENDS - Started 11:48 9/03/2020
TRENDS = ['#coronavirus', 'cases', 'spread', 'NHS']
streamer.filter(track= TRENDS, locations=LOCATIONS, languages=['en'])



