# Databricks notebook source
# MAGIC %md
# MAGIC # TwitterStream to S3 / DBFS
# MAGIC 
# MAGIC * [DLT Pipeline](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#joblist/pipelines/267b91c1-52f0-49e3-8b77-9a50b34f5d39)
# MAGIC * [Huggingface Sentiment Analysis](https://data-ai-lakehouse.cloud.databricks.com/?o=2847375137997282#notebook/2328309019401991)

# COMMAND ----------

# should use databricks secrets and the CLI to store and retrieve those keys in a safe way.


# in my demo, I read in the keys from another notebook in the cell below (which can be savely removed or commented out)


# COMMAND ----------

#!/databricks/python3/bin/python -m pip install --upgrade pip

# COMMAND ----------

!pip install tweepy jsonpickle

# COMMAND ----------

!pip install git+https://github.com/tweepy/tweepy.git@master


# COMMAND ----------

import tweepy
import calendar
import time
import jsonpickle
import sys


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
client = tweepy.StreamingClient(bearer_token=BEARER_TOKEN,wait_on_rate_limit=True)
# client = tweepy.StreamingClient(bearer_token=TWITTER_BEARER_TOKEN)


api = tweepy.API(auth, wait_on_rate_limit=True, timeout=60)

print(f'Twitter sc name: {api.verify_credentials().screen_name}')


# Subclass Stream  
class TweetStream(tweepy.StreamingClient):

    def __init__(self, filename):
        tweepy.StreamingClient.__init__(self, BEARER_TOKEN)
        self.filename = filename
        self.text_count = 0
        self.tweet_stack = []
    
    def on_connect(self):
        print ('connected')


    def on_tweet(self, status):
        print(' on tweet '+status.text)
        self.text_count = self.text_count + 1
        self.tweet_stack.append(status)
    
        # when to print
        if (self.text_count % 1 == 0):
            print(f'retrieving tweet {self.text_count}: {status.text}')
            time.sleep(2)

      
        # how many tweets to batch into one file
        if (self.text_count % 5 == 0):
            self.write_file()
            self.tweet_stack = []

        # hard exit after collecting n tweets
        if (self.text_count == 10):
            tweet_stream.write_file()
            raise Exception("Finished job")

    def write_file(self):
        print(' on write finish  ')
        file_timestamp = calendar.timegm(time.gmtime())
        fname = self.filename + '/tweets_' + str(file_timestamp) + '.json'


        f = open(fname, 'w')
        for tweet in self.tweet_stack:
            print('tweeeting BEGIN')
            print(tweet)
            print('tweeeting END')
#             f.write(tweet)
            f.write(jsonpickle.encode(tweet))
#             f.write('\n')
        f.close()
        print("Wrote local file ", fname)

    def on_error(self, status_code):
        print("Error with code ", status_code)
        tweet_stream.disconnect()
        sys.exit()

    def delete_rules(self, client):
        # Initialize instance of the subclass
        previousRules = client.get_rules().data
#         print(' previousRules ' + previousRules)
        if previousRules:
            print(' deleting ')
            client.delete_rules(previousRules)
    
tweet_stream = TweetStream("/dbfs/data/twitter_dataeng2")
tweet_stream.delete_rules(client)
tweet_stream.add_rules(tweepy.StreamRule("lang:en"))
# tweet_stream.add_rules(tweepy.StreamRule("lang: au"))
tweet_stream.add_rules(tweepy.StreamRule("lang:de"))
tweet_stream.add_rules(tweepy.StreamRule("lang:es"))
tweet_stream.add_rules(tweepy.StreamRule("DLT"))
tweet_stream.add_rules(tweepy.StreamRule("data science"))
tweet_stream.add_rules(tweepy.StreamRule("Databricks"))
tweet_stream.add_rules(tweepy.StreamRule("AI/ML"))
tweet_stream.add_rules(tweepy.StreamRule("data lake"))
tweet_stream.add_rules(tweepy.StreamRule("machine learning"))
tweet_stream.add_rules(tweepy.StreamRule("lakehouse"))
tweet_stream.add_rules(tweepy.StreamRule("Delta Live Tables"))


# Filter realtime Tweets by keyword
try:
    tweet_stream.filter()
#     tweet_stream.filter(languages=["en","de","es"],track=["Databricks", "data science","AI/ML","data lake","machine learning","lakehouse","DLT","Delta Live Tables"])




except Exception as e:
    print("some error ", e)
    print("Writing out tweets file before I have to exit")
    tweet_stream.write_file()
finally:
    print("Downloaded tweets ", tweet_stream.text_count)
    tweet_stream.disconnect()

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a DBFS directory, check for number of files, deletes a certain number of files ...

# COMMAND ----------

# create a directory to buffer the streamed data
!mkdir "/dbfs/data/twitter_dataeng2"

# COMMAND ----------

# create a directory to buffer the streamed data
!ls -l /dbfs/data/twitter_dataeng2 | wc

# COMMAND ----------

files = dbutils.fs.ls("/data/twitter_dataeng2")
# del = 400
print(f'number of files: {len(files)}')
print(f'number of files to delete: 400')


for x, file in enumerate(files):
  # delete n files from directory
  if x < 400 :
    # print(x, file)
    dbutils.fs.rm(file.path)

    
# use dbutils to copy over files... 
# dbutils.fs.cp("/data/twitter_dataeng/" +f, "/data/twitter_dataeng2/")

# COMMAND ----------


