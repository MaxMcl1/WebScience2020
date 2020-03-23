# Task 2a
# Using K Means Clustering Algorithm to group tweets

import tweepy
import re
import json
import time
import config
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
import os
import numpy as np 
import pandas as pd 
from collections import Counter
from sklearn.cluster import KMeans
from sklearn.externals import joblib
from pymongo import MongoClient

LOAD_KMEANS = False
EVALUATION = False
SAVE_GRAPH = False

client = MongoClient('localhost', 27017)
db = client['local']
collection = db['Crawler 1b']

tweets = []
hashtags = []
tweet_text = []
tweet_mongo_id = []

def clean_tweet(tweet): 
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet).split())

for tweet in collection.find({}, {"text": 1, "_id": 1, "entities": 1}):
    clean_tweet(tweet["text"])
    if (tweet["entities"].get("hashtags")) != []:
        # need to handle tweets with multiple hashtags
        hashtags.append(tweet["entities"].get("hashtags")[0].get("text"))
    else:
        hashtags.append("")
    tweet_text.append(tweet["text"])
    tweet_mongo_id.append(tweet["_id"])
    tweets.append(tweet)


tfidf_vec = TfidfVectorizer() 
tfidf_matrix = tfidf_vec.fit_transform(tweet_text)

num_clusters = 10

km = KMeans(n_clusters = num_clusters)
km.fit(tfidf_matrix)

print(Counter(hashtags))

feature_names = tfidf_vec.get_feature_names()

clusters = km.labels_.tolist()

pd.set_option('display.max_rows', None)
    
tweets = {'mongo_id': tweet_mongo_id, 'tweet_text': tweet_text, 'cluster':clusters, 'hashtags': hashtags}
df = pd.DataFrame(tweets, index = [clusters], columns = ['mongo_id', 'cluster', 'tweet_text', 'hashtags'])

#print(df)

group_counts = df['cluster'].value_counts().sort_index()

def extract_place(row):
    return row["name"]

bar_width = 0.3
plt.figure()
plt.bar(np.arange(num_clusters)-bar_width, group_counts, label="Total Tweets", width=-bar_width, align="edge")
plt.legend()
plt.xticks(range(num_clusters))
plt.title("Tweet Cluster")
plt.xlabel("Cluster")
plt.ylabel("Number of Tweets")
plt.tight_layout()

plt.show()

    



