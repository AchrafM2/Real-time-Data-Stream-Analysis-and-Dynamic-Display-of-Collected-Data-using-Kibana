from kafka import KafkaConsumer 
from kafka import KafkaProducer 
from confluent_kafka import Producer 
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Define Kafka broker address
bootstrap_servers2 = 'localhost:9892'


# Download necessary resources (run only once)
nltk.download('vader_lexicon')


#Kafka consumer settings
kafka_bootstrap_servers = 'localhost:9092'
kafka_topicA = 'twitter1'
kafka_topicB = "labels" 
# Kafka consumer
consumer= KafkaConsumer (kafka_topicA, bootstrap_servers=kafka_bootstrap_servers)


#Configuration for the Kafka producer
producer_config = {
    "bootstrap.servers": bootstrap_servers2,
}


# I can add more configuration options here


#the second kafka producer
producer2= Producer(producer_config)

#Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

#Consume and process tweets from the twitter kafka topic
print("Starting to process tweets...")

try:
    for message in consumer:
        tweet_text = message. value.decode ('utf-8')
        sentiment_scores = analyzer.polarity_scores (tweet_text)

        # Process sentiment analysis result
        sentiment_score = sentiment_scores [ 'compound' ]
        if sentiment_score >= 0.05:
            sentiment_label = 'Positive'
        
    
        elif sentiment_score <= -0.05:
            sentiment_label = 'Negative'
        
    
        else:
            sentiment_label = 'Neutral'
        

        print("Sentiment Label:", sentiment_label)

    try:
        message_key="key1"
        producer2.produce(kafka_topicB, key=message_key, value=sentiment_label)
        producer2.flush()
        print("Sentiment label sent successfully to kafka topic: ", kafka_topicB)

    except Exception as e:
        print("Error sending sentiment label:", e)

except Exception as e:
    print("An exception occured:", e)

    
