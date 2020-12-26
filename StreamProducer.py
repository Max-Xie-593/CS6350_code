from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "J9Ldk8c3vyBSQAl4ueo2UcjH1"
consumer_secret = "JUJnwAiBuRoFVlQ3hvQfS7Tk1AJnwMpVH5IS8NCpWyDtQeQGoc"
access_token = "1250574629992902656-V3PfyhlWbEIAWb2pUywyowhTJOHs5W"
access_secret = "olLDgl8Stx9OnHQNE2ll2sWGdsQmRKNLv2Jsn3SOjI8UD"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=['#trump'])