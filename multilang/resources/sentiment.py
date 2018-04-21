import storm
import boto3

class SentimentBolt(storm.BasicBolt):

    def process(self, tup):
        comprehend = boto3.client(service_name='comprehend', region_name='us-west-2')
        text = tup.values[1]
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        storm.logInfo("SentimentBolt got: " + str(sentiment) + " for text " + text)

        score = sentiment["SentimentScore"]["Positive"] - sentiment["SentimentScore"]["Negative"]

        out = [tup.values[0], str(score), tup.values[2], tup.values[3]]
        storm.emit(out)

SentimentBolt().run()