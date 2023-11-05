import kafka
from kafka import KafkaProducer
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener



#twitterAPI
key= "cAOnn79TEwO6U5A54vHJ8FBLA"
key_secret="qZlbtrIBmT66a3DD6tfsoFXhgZmJHIeGqRF6jeCiv4DdpeHsAe"
access_token="1506775653923840002-je8MepECvfpCcP2kabdhpJInb8qTYp"
acess_secret= "apgHvys0y32rPax7QWpuyGpsWeIOJlvf2alm0S3IBM4Uu"


#kafka producer setting
kafka_bootstrap_servers="localhost:9092"
kafka_topic='twitter1'


#Twitter API Authentification
auth = OAuthHandler(key,key_secret)
auth.set_access_token(access_token,acess_secret)
api=tweepy.API(auth)

#kafka producer
producer=KafkaProducer(bootsrap_servers=kafka_bootstrap_servers)

#kafka producer function

class KaTkatreamListener(tweepy.streamListener):
    def on_status (self, status):
        #send tweets text to kafka topic
        producer.send(kafka_topic, status. text.encode('utf-8'))
        print(status.text)
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        
#Start Twitter stream and send tweets to kafka

stream_listener=KaTkatreamListener()
stream=tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=['#messi'])


    





