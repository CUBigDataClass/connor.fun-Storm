import storm
import numpy
import sklearn

class SentimentBolt(storm.Bolt):
    def initialize(self, conf, context):
        # Load pickle
        self._conf = conf
        self._context = context
        #self.comp = boto3.client(service_name='comprehend', region_name='us-west-2')

    def process(self, tup):
        #import sys
        text = tup.values[1]
        #sentiment = self.comp.detect_sentiment(Text=text, LanguageCode='en')

        # Classify
        score = 0.01 #sentiment["SentimentScore"]["Positive"] - sentiment["SentimentScore"]["Negative"]
        storm.logInfo("SentimentBolt got: " + str(score) + " for text " + text)

        tup.values[1] = score
        storm.emit(tup.values)

SentimentBolt().run()