import storm
import boto3

class SentimentBolt(storm.BasicBolt):
    def initialize(self, conf, context):
        self.comp = boto3.client(service_name='comprehend', region_name='us-west-2')

    def process(self, tup):
        text = tup.values[1]
        sentiment = self.comp.detect_sentiment(Text=text, LanguageCode='en')

        score = sentiment["SentimentScore"]["Positive"] - sentiment["SentimentScore"]["Negative"]
        storm.logInfo("SentimentBolt got: " + str(score) + " for text " + text)

        tup.values[1] = score
        storm.emit(tup.values)

SentimentBolt().run()