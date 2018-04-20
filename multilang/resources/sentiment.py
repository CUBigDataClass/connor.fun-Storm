import storm
import boto3

class SentimentBolt(storm.BasicBolt):
    def process(self, tup):
        comprehend = boto3.client(service_name='comprehend', region_name='us-west-2')
        text = tup[0]
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='end')

SentimentBolt().run()