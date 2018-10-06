import sys
import numpy as np
import time
from itertools import combinations
from pyspark import SparkContext

sc = SparkContext(appName="SON")

start = time.time()

def get_candidate_set(frequent_items,k):
    l = []
    if k == 2:
        for i,e1 in enumerate(frequent_items):
            for j,e2 in enumerate(frequent_items):
                if j>i:
                    pair = e1|e2
                    if len(pair) == k and pair not in l:
                        l.append(pair)
    else:
        for i,e1 in enumerate(frequent_items):
            for j,e2 in enumerate(frequent_items):
                if j>i:
                    common = e1.intersection(e2)
                    if len(common) == k-2:
                        pair = e1|e2
                        if pair not in l:
                            pairs = list(combinations(pair,k-1))
                            c = 0
                            for p in pairs:
                                if c == k-2:
                                    break
                                if common.issubset(p) == False:
                                    if set(p) in frequent_items:
                                        c = c + 1
                            if c == k-2:
                                l.append(pair)
    return l

def apriori(iterator):
    baskets = list(iterator)
    support_part = np.floor(float(support)/numPartitions)
    k = 2
    d = {}
    frequent_items = []
    frequent_itemset = []
    
    for basket in baskets:
        for item in basket:
            if item not in d:
                d[item] = 1
            else:
                d[item] = d[item] + 1
    
    for item in d:
        if d[item] >= support_part:
            frequent_items.append(item)
    
    frequent_itemset = [(i,1) for i in frequent_items]
    frequent_items = [{i} for i in frequent_items]
    
    while (len(frequent_items)>0):
        candidate_set = get_candidate_set(frequent_items,k)
        frequent_items = []
        for candidate in candidate_set:
            c = 0
            for basket in baskets:
                if candidate.issubset(basket):
                    c = c + 1
            if c >= support_part:
                frequent_items.append(candidate)
        
        if len(frequent_items)>0:
            frequent_itemset = frequent_itemset + [(tuple(i),1) for i in frequent_items]
        k = k + 1
        
    return(frequent_itemset)

def count_occurances(iterator,candidate_itemset):
    baskets = list(iterator)
    candidate_local = []
    for candidate in candidate_itemset:
        c = 0
        if type(candidate) != tuple:
            candidate = (candidate,)
        for basket in baskets:
            flag =  all(item in basket  for item in candidate)
            if flag == True:
                c = c + 1
        candidate_local.append((candidate,c))
    
    return(candidate_local)

case = int(sys.argv[1])
input_file = sys.argv[2]
support = int(sys.argv[3])

if input_file.find('Small2') != -1:
    output_file = 'Nikhit_Mago_SON_Small2.case{}-{}.txt'.format(case,support)
elif input_file.find('MovieLens.Small') != -1:
    output_file = 'Nikhit_Mago_SON_MovieLens.Small.case{}-{}.txt'.format(case,support)
elif input_file.find('MovieLens.Big') != -1:
    output_file = 'Nikhit_Mago_SON_MovieLens.Big.case{}-{}.txt'.format(case,support)
elif input_file.find('Small1') != -1:
    output_file = 'Nikhit_Mago_SON_Small1.case{}-{}.txt'.format(case,support)
else:
    print('Please read instructions from Nikhit_Mago_Description.pdf and give correct name to file.')
    sys.exit()

if input_file.find('Small2') != -1:
    data = sc.textFile(input_file,minPartitions=1)
else:
    data = sc.textFile(input_file)
    
header = data.first()

if case == 1:
    baskets = data.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).groupByKey().map(lambda x: list(set(x[1])))
            
elif case == 2:
    baskets = data.filter(lambda x: x!=header).map(lambda x: x.split(',')).map(lambda x: (int(x[1]),int(x[0]))).groupByKey().map(lambda x: list(set(x[1])))
    
numPartitions = baskets.getNumPartitions()

#MapReduce Phase 1
candidate_itemset = baskets.mapPartitions(apriori).groupByKey().map(lambda x:x[0]).collect()

#MapReduce Phase 2
candidate_set_counts = baskets.mapPartitions(lambda x: count_occurances(x,candidate_itemset)).reduceByKey(lambda x,y: x+y).collect()

frequent_items = []
for item in candidate_set_counts:
    if item[1] >= support:
        frequent_items.append(item[0])

max_len = 0
for item in frequent_items:
    if len(item) > max_len:
        max_len = len(item)

d = {i:[] for i in range(1,max_len+1)}
for item in frequent_items:
    d[len(item)].append(tuple(sorted(item)))

with open(output_file,'wb') as file_write:
    for key in d:
        d[key].sort()
        file_write.write(str(d[key])[1:-1].replace(',)',')'))
        file_write.write('\n\n')
file_write.close()

end = time.time()

print("\n\n\nTask Completed in: {} seconds\n\n\n".format(np.round((end-start),2)))