import csv
d = {}
i = 1
with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/SportsTest.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        d[row[0]] = i
        i = i + 1
with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/BusinessTest.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        d[row[0]] = i
        i = i + 1
with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/MediaTest.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        d[row[0]] = i
        i = i + 1
with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/PoliticsTest.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        d[row[0]] = i
        i = i + 1
        
#print(d)


dz = {}
i = 0
dz['Sports'] = i
i = i + 1
dz['Politics'] = i
i = i + 1
dz['Business'] = i
i = i + 1
dz['Media'] = i

#print(dz)

def return_dict(line):
    d1 = {}
    #i = 0
    with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/SportsTest.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            d1[row[0]] = 0
    with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/BusinessTest.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            d1[row[0]] = 0
    with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/MediaTest.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            d1[row[0]] = 0
    with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/PoliticsTest.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            d1[row[0]] = 0
            
    words = line.split(' ')
    
    for i in range(0,len(words)):
        if words[i] != ' ':
            if words[i] in d.keys():
                counter = d1[words[i]]
                counter = counter + 1
                d1[words[i]] = counter
            else:
                continue
                
    return d1

sol = ""
with open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/Testsize2/testlistsize2.csv', 'rb') as f:
    reader = csv.reader(f, delimiter = ',')
    reader.next()
    for row in reader:
        line = row[0]
        diction1 = return_dict(line)
        #add this dictionaries data to the output file
        new_d = {}
        
        for key in diction1.keys():
            if key in d:
                new_d[d[key]] = diction1[key]
        
        for key in d.keys():
            if d[key] in new_d:
                continue
            else:
                new_d[d[key]] = 0
        #add to sol the new data from the article
        sol = sol + row[1]
        
        for key in new_d.keys():
            sol = sol + " " + str(key) + ":" + str(new_d[key]) 
        
        sol = sol + "\n"
        


print(sol)

file123 = open('C:/Users/dues1/Desktop/dataset-lab3/dataset-lab3/Test/MyTest3.txt','w')
file123.write(sol)
file123.close()
        
#        print(diction1)
        
        


