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
    
    def predict(self, newTweetTexts):
        feats = []
        for text in newTweetTexts:
            feats.append(self.extract_features(text))
            
        feat_vec = self.vectorizer.transform(feats)
        
        return self.sentiment.decision_function(feat_vec)

class SentimentBolt(storm.BasicBolt):
    def initialize(self, conf, context):
        self.tweets = []
        with open(MODEL_NAME,'rb') as f:
            self.model = pickle.load(f)

    def process(self, tup):
        self.tweets.append(tup)

        if len(self.tweets) > 50:
        
            texts = [tweet.values[1] for tweet in self.tweets]
            scores = self.model.predict(texts)
        
            for ii, score in enumerate(scores):
                self.tweets[ii].values[1] = score
                storm.emit(self.tweets[ii].values)

            self.tweets = []

SentimentBolt().run()