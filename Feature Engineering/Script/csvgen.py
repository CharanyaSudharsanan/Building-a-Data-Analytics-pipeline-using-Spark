# -*- coding: utf-8 -*-
"""
Created on Fri May 11 16:57:09 2018

@author: dues1
"""

import sys
import os
import csv
#import nltk
#nltk.download('stopwords')
#from nltk import pos_tag
#nltk.download('averaged_perceptron_tagger')
#from nltk.corpus import stopwords
#from nltk.tokenize import word_tokenize
w = 2
h = 10
newlist = [[0 for x in range(w)] for y in range(h)]
newlist[0][0] = "FileContent"
newlist[0][1] = "Type"
i = 1
    
for file1 in os.listdir("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/SportsTest"):            
    if file1.endswith(".txt"):
        with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/SportsTest/"+file1) as f:
            print("file is:",f)
            filecontents = f.read()
            newlist[i][0] = filecontents
            newlist[i][1] = "1";
            i = i + 1
                
    
for file in os.listdir("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/BusinessTest"):            
    if file.endswith(".txt"):
        with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/BusinessTest/"+file) as f:
            filecontents = f.read()
            newlist[i][0] = filecontents
            newlist[i][1] = "2";
            i = i + 1
                
    
for file in os.listdir("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/MediaTest"):            
    if file.endswith(".txt"):
        with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/MediaTest/"+file) as f:
            filecontents = f.read()
            newlist[i][0] = filecontents
            newlist[i][1] = "3"
            i = i + 1
                
    
for file in os.listdir("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/PoliticsTest"):            
    if file.endswith(".txt"):
        with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/PoliticsTest/"+file) as f:
            filecontents = f.read()
            newlist[i][0] = filecontents
            newlist[i][1] = "4";
            i = i + 1
with open("C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/testlistsize2.csv", "w") as f:
    writer = csv.writer(f)   
    writer.writerows(newlist)