import os
import sys
import graphframes
import pyspark
import itertools
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from graphframes import *
conf = SparkConf().setAppName("Log Analyzer")
sc = pyspark.SparkContext('local[*]', 'wordCount')
sqlContext = SQLContext(sc)

s = int(sys.argv[1])
path = sys.argv[2]
output_1 = sys.argv[3]


rdd = sc.textFile(path)
#rdd = sc.textFile('/Users/yajiewang/Downloads/553hw4/ub_sample_data.csv')

header = rdd.first()
data = rdd.filter(lambda row: row != header).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).collect()

vertices ={}
edgelist = []
userdict = {}
for i in range(0,len(data)):
    userid = data[i][0]
    itemid = data[i][1]
    if userid not in userdict:
        userdict[userid] = set()
    userdict[userid].add(itemid)

#Find Edges
for i in itertools.combinations(userdict.keys(), 2):
    if len(userdict[i[0]].intersection(userdict[i[1]])) >= s:
        edgelist.append(tuple(sorted(i)))

def add_edge( u, v):
        if u not in vertices:
            vertices[u] = list()
        if v not in vertices:
            vertices[v] = list()
        vertices[u].append(v)
        vertices[v].append(u)
for e in edgelist:
    add_edge(e[0], e[1])

#print(userdict)

edge = sc.parallelize(edgelist)
vert = sc.parallelize(vertices.keys()).map(lambda x:(x, ))
#print(vertics.collect())

v=sqlContext.createDataFrame(vert,["id"])
e=sqlContext.createDataFrame(edge,["src", "dst"])



g = GraphFrame(v,e)

result = g.labelPropagation(maxIter=5)
result = result.rdd.map(tuple)
com = result.map(lambda x:(x[1],x[0])).groupByKey().mapValues(lambda x: sorted(list(x))).map(lambda x: (len(x[1]), x[1]))\
    .sortBy(lambda x: (x[0],x[1][0])).map(lambda x: tuple(x[1])).collect()

with open(output_1, "w") as file:
    for i in com:
        file.write("\'{}\'\n".format("\', \'".join([str(j) for j in i]).strip(",")))
