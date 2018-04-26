import storm
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import BaggingClassifier
from sklearn import svm
import pickle

MODEL_NAME = 'modelv01.pickle'

class GrantModel():
    def train(self, features, labels):
        cores = 8
        self.vectorizer = DictVectorizer(sparse=True)
        self.sentiment = BaggingClassifier(svm.LinearSVC(), max_samples=1.0/cores,
                                           n_estimators=cores, n_jobs=cores)
        
        train_vec = self.vectorizer.fit_transform(features)
        self.sentiment.fit(train_vec, labels)
        
    def extract_features(self, tweet):
        feats = {}
        tweet = tweet.split(' ')
        feats['NUMCAPS'] = 0
        for j in range(len(tweet)):
            word = tweet[j]

            if len(word) > 0 and word[0] != '@':
                feats['WORD='+word.lower()] = 1
                feats['NUMCAPS'] += sum(1 for char in word if char.isupper())
        return feats
    
    def predict(self, newTweet):
        feats = self.extract_features(newTweet)
        feat_vec = self.vectorizer.transform(feats)
        
        return self.sentiment.predict(feat_vec)[0]

class SentimentBolt(storm.BasicBolt):
    def initialize(self, conf, context):
        with open(MODEL_NAME,'rb') as f:
            self.model = pickle.load(f)

    def process(self, tup):
        text = tup.values[1]
        score = self.model.predict(text)
        
        storm.logInfo("SentimentBolt got: " + str(score) + " for text " + text)

        tup.values[1] = score
        storm.emit(tup.values)

SentimentBolt().run()