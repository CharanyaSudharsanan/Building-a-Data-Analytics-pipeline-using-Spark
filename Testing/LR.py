'''
The following links/sources have been used to write/understand/refer this code:

https://stackoverflow.com/questions/32284620/how-to-change-a-dataframe-column-from-string-type-to-double-type-in-pyspark
https://towardsdatascience.com/multi-class-text-classification-with-pyspark-7d78d022ed35
https://github.com/dichen001/CSC522-Spark/blob/master/pre-process.py
https://yidatao.github.io/2016-03-23/Document-Classification-using-pyspark/

'''


from __future__ import print_function

import shutil

from pyspark import SparkContext, SparkConf
# $example on$
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.ml.feature import CountVectorizer
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql.types import *
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import LogisticRegression
from nltk.corpus import stopwords 
import pandas as pd
from pyspark.ml.feature import RegexTokenizer,StopWordsRemover
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


mystopwords = ["advertisement","percent","space","years","another","first","second","third","one","might","two","three","think","right","better","people","women","family","person","trump","woman","article","state","still","including","college","united","states","new","york","later","better","president","office","working","white","house","chief","start","small","world","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"]
nltkstop = stopwords.words('english')
finalstopwords = mystopwords + nltkstop

conf = SparkConf().setMaster("local").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)
sqlContext = sql.SQLContext(sc)

#test code starts here -- part 5

lines1 = sc.textFile("/Users/prachishah/Desktop/final.csv")

parts1 = lines1.map(lambda l: l.split(","))

people1 = parts1.map(lambda p: (p[0], p[1].strip()))

schemaString = "FileContent label"


Tfields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
Tschema = StructType(Tfields)

TschemaP = sqlContext.createDataFrame(people1, Tschema)
TschemaPeople = TschemaP.withColumn("label", TschemaP["label"].cast(DoubleType()))

regexTokenizer = RegexTokenizer(inputCol="FileContent", outputCol="words", pattern="\\W")
stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(nltkstop)

regexer = regexTokenizer.transform(TschemaPeople)
stop = stopwordsRemover.transform(regexer)

#tokenizer = Tokenizer(inputCol="filtered", outputCol="words")
#wordsData = tokenizer.transform(stop)
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures")
#hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)

featurizedData = hashingTF.transform(stop)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
seenData = idfModel.transform(featurizedData)


(trainingData1, testData1) = seenData.randomSplit([0.6, 0.4], seed = 100)

lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
lrModel = lr.fit(trainingData1)
predictions1 = lrModel.transform(testData1)

predictions1.select("FileContent","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show( truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

print("the accuracy is:", evaluator.evaluate(predictions1))

#unseen data
linest = sc.textFile("/Users/prachishah/Desktop/docmatrix.csv")

partst = linest.map(lambda l: l.split(","))

peoplet = partst.map(lambda p: (p[0], p[1].strip()))


schemaname = "FileContent label"
Tfieldst = [StructField(field_name, StringType(), True) for field_name in schemaname.split()]
Tschemat = StructType(Tfieldst)

TschemaPt = sqlContext.createDataFrame(peoplet, Tschemat)
TschemaPeoplet = TschemaPt.withColumn("label", TschemaPt["label"].cast(DoubleType()))

step1 = regexTokenizer.transform(TschemaPeoplet)
step2 = stopwordsRemover.transform(step1)
step3 = hashingTF.transform(step2)
idfModel = idf.fit(step3)
unseenData = idfModel.transform(step3)
predictions = lrModel.transform(unseenData)
print("the unseen data accuracy is:", evaluator.evaluate(predictions))


