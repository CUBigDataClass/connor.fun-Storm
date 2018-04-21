import storm
import boto3

class SentimentBolt(storm.BasicBolt):

    def process(self, tup):
        comprehend = boto3.client(service_name='comprehend', region_name='us-west-2')
        text = tup.values[1]
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')

        score = sentiment["SentimentScore"]["Positive"] - sentiment["SentimentScore"]["Negative"]
        storm.logInfo("SentimentBolt got: " + str(score) + " for text " + text)

        tup.values[1] = score
        storm.emit(tup.values)

SentimentBolt().run()