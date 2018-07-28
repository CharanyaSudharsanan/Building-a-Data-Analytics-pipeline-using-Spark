# -- coding: utf-8 --
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark import SparkConf, SparkContext

from operator import add
import sys
import os# -- coding: utf-8 --
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark import SparkConf, SparkContext

from operator import add
import sys
import os
import csv
import nltk
nltk.download('stopwords')
from nltk import pos_tag
nltk.download('averaged_perceptron_tagger')
from nltk.corpus import stopwords
#from nltk.tokenize import word_tokenize
## Constants
APP_NAME = " HelloWorld of Big Data"
##OTHER FUNCTIONS/CLASSES
stopwords = set(stopwords.words('english'))

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext.getOrCreate()
    for file1 in os.listdir("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Wordcount/Wordcount/Sports150"):
        if file1.endswith(".txt"):
            textRDD = sc.textFile("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Wordcount/Wordcount/"+file1)
            words = textRDD.flatMap(lambda x: x.split(' '))
             #data pre-processing
            stopwords = ["Advertisement","percent","space","years","another","first","second","third","one","might","two","three","think","right","better","people","women","family","person","trump","woman","article","state","still","including","college","united","states","new","york","later","better","president","office","working","white","house","chief","start","small","world","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"," "]
            filt = words.filter(lambda x: x not in stopwords)
            wordsf = filt.map(lambda x: (x, 1))
   
        wordcount = wordsf.reduceByKey(add).collect()
        nfword = sorted(wordcount,key=lambda x: x[1], reverse = True)
   
    word = []

    for list1 in nfword:
        if list1[0] not in stopwords:
            if len(list1[0]) > 4 :
                sent = pos_tag([list1[0]])
                print('sent ',sent)
                
                if(sent[0][1] != 'NN' and sent[0][1] !='NNP' and sent[0][1] !='PRP$' and sent[0][1] !='VBZ' and sent[0][1] != 'CD' and sent[0][1] !='JJ' and sent[0][1] !='JJR' and sent[0][1] !='JJS' and sent[0][1] !='MD' and sent[0][1] !='PDT' and sent[0][1] !='POS' and sent[0][1] !='RB' and sent[0][1] !='RBS' and sent[0][1] !='TO' and sent[0][1] !='VB' and sent[0][1] !='VBD' and sent[0][1] !='VB' and sent[0][1] !='VBD' and sent[0][1] !='VBG' and sent[0][1] !='VBN' and sent[0][1] !='VBP' and sent[0][1] !='VBZ' and sent[0][1] !='WDT' and sent[0][1] !='WP' and sent[0][1] !='WP$' and sent[0][1] !='WRB' and sent[0][1] != 'IN' and sent[0][1] != 'CD' and sent[0][1] != 'JJ'):
                    word.append(list1) 
#   totalwords = sum(row[1] for row in word) #total number of words in all files
   
   #new list dimensions to save top 25 words with their probablity
    w,h = 2, 25 #2 is no of columns and 25 is top words
    newlist = [[0 for x in range(w)] for y in range(h)]
    for x in range(h):
        newlist[x][0] = word[x][0]
        newlist[x][1] = word[x][1]

    with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Wordcount/Wordcount/"+"Sports_05112018.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(newlist)

import csv
import nltk
nltk.download('stopwords')
from nltk import pos_tag
nltk.download('averaged_perceptron_tagger')
from nltk.corpus import stopwords
#from nltk.tokenize import word_tokenize
## Constants
APP_NAME = " HelloWorld of Big Data"
##OTHER FUNCTIONS/CLASSES
stopwords = set(stopwords.words('english'))

def myMain(sc, filepath):
   for file in os.listdir(filepath):
      if file.endswith(".txt"):
         textRDD = sc.textFile(filepath)
         words = textRDD.flatMap(lambda x: x.split(' '))
         #data pre-processing
         stopwords = ["Advertisement","percent","space","years","another","first","second","third","one","might","two","three","think","right","better","people","women","family","person","trump","woman","article","state","still","including","college","united","states","new","york","later","better","president","office","working","white","house","chief","start","small","world","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out", "very", "having", "with", "they", "own", "an", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself", "other", "off", "is", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "are", "we", "these", "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above", "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will", "on", "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just", "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "a", "by", "doing", "it", "how", "further", "was", "here", "than"," "]
         filt = words.filter(lambda x: x not in stopwords)
         wordsf = filt.map(lambda x: (x, 1))
   wordcount = wordsf.reduceByKey(add).collect()
   nfword = sorted(wordcount,key=lambda x: x[1], reverse = True)
   
   word = []

   for list1 in nfword:
       if list1[0] not in stopwords:
           if len(list1[0]) > 4 :
               sent = pos_tag([list1[0]])
               print('sent ',sent)
               if(sent[0][1] != 'NN' and sent[0][1] !='NNP' and sent[0][1] !='PRP$' and sent[0][1] !='VBZ' and sent[0][1] != 'CD' and sent[0][1] !='JJ' and sent[0][1] !='JJR' and sent[0][1] !='JJS' and sent[0][1] !='MD' and sent[0][1] !='PDT' and sent[0][1] !='POS' and sent[0][1] !='RB' and sent[0][1] !='RBS' and sent[0][1] !='TO' and sent[0][1] !='VB' and sent[0][1] !='VBD' and sent[0][1] !='VB' and sent[0][1] !='VBD' and sent[0][1] !='VBG' and sent[0][1] !='VBN' and sent[0][1] !='VBP' and sent[0][1] !='VBZ' and sent[0][1] !='WDT' and sent[0][1] !='WP' and sent[0][1] !='WP$' and sent[0][1] !='WRB' and sent[0][1] != 'IN' and sent[0][1] != 'CD' and sent[0][1] != 'JJ'):
                   word.append(list1) 
#   totalwords = sum(row[1] for row in word) #total number of words in all files
   
   #new list dimensions to save top 25 words with their probablity
   w,h = 2, 25 #2 is no of columns and 25 is top words
   newlist = [[0 for x in range(w)] for y in range(h)]
   for x in range(h):
      newlist[x][0] = word[x][0]
      newlist[x][1] = word[x][1]

   with open(filepath+"Sports1501_Output.csv", "w") as f:
      writer = csv.writer(f)
      writer.writerows(newlist)
      

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("local[*]")
   sc   = SparkContext(conf=conf)
   textfile = sys.argv[1]
   # Execute Main functionality
   #main(sc,textfile)
   myMain(sc, textfile)