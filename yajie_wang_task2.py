from collections import defaultdict

import pyspark
import time
import os, sys, csv
start = time.time()
import itertools

s = int(sys.argv[1])
path = sys.argv[2]
output_1 = sys.argv[3]
output_2 = sys.argv[4]

sc = pyspark.SparkContext('local[*]', 'wordCount')
#rdd = sc.textFile(sys.argv[1])
rdd = sc.textFile(path)


class Graph:
    #B = {}
    vertices = {}
    '''def __init__(self, edges):
        self.m = len(edges)
        for e in edges:
            self.add_edge(e[0], e[1])
        n = len(self.vertices)
        for i in range(0, n):
            self.Aij[i] = dict()
            for j in range(0, n):
                self.Aij[i][j] = 0
        for u, v in enumerate(self.vertices):
            self.dicB[v] = u
        for u in self.vertices:
            for v in self.vertices[u]:
                self.Aij[self.dicB[u]][self.dicB[v]] = 1'''

    def add_edge(self, u, v):
        if u not in self.vertices:
            self.vertices[u] = list()
        if v not in self.vertices:
            self.vertices[v] = list()
        self.vertices[u].append(v)
        self.vertices[v].append(u)
    '''def initBelong(self):
        for u in self.vertices:
            self.belongsto[u] = 1

    def degree(self, node):
        return len(self.vertices[node])

    def calcMatrix(self):
        for u in self.vertices:
            if u not in self.Bij:
                self.Bij[u] = dict()
            for v in self.vertices:
                self.Bij[u][v] = self.Aij[self.dicB[u]][self.dicB[v]] - float(self.degree(u) * self.degree(v)) / (2 * self.m)'''



    def bfs(self, root):
        visited = [root]
        parent = defaultdict(set)
        treeLevel= defaultdict(set)
        value = defaultdict(float)
        parent[root] = None
        value[root] = 1
        treeLevel[root] = 0
        weight = defaultdict(float)
        for v in self.vertices[root]:
            visited.append(v)
            value[v] = 1
            parent[v] = [root]
            treeLevel[v] = 1
        i = 1
        while i <= len(visited) - 1:
            node = visited[i]
            weight[node] = 1
            link_node = self.vertices[node]
            value_i = 0
            for v in parent[node]:
                value_i += value[v]
            value[node] = value_i
            for v in link_node:
                if v not in visited:
                    visited.append(v)
                    treeLevel[v] = treeLevel[node] + 1
                    parent[v] = [node]
                else:
                    if treeLevel[v] == treeLevel[node] + 1:
                        parent[v].append(node)
            i += 1
        visited_r = list(reversed(visited))
        for a in visited_r[:-1]:
            for b in parent[a]:
                k = tuple(sorted([a, b]))
                v = weight[a] * (value[b] / value[a])
                weight[b] += v
                yield (k, v)

    def trav(self):
        for root in self.vertices:
            self.bfs(root)

header = rdd.first()
data = rdd.filter(lambda row: row != header).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).collect()

edgelist = []
userdict = {}
for i in range(0,len(data)):
    userid = data[i][0]
    itemid = data[i][1]
    if userid not in userdict:
        userdict[userid] = set()
    userdict[userid].add(itemid)
#print(userdict)

#Find Edges
for i in itertools.combinations(userdict.keys(), 2):
    if len(userdict[i[0]].intersection(userdict[i[1]])) >= s:
        edgelist.append(tuple(sorted(i)))
dic_u = sc.parallelize(edgelist).flatMap(lambda s: [(s[0], [s[1]]),(s[1], [s[0]])]).reduceByKey(lambda x,y: x+y).collectAsMap()
user_list = sorted(dic_u.keys())
btns = defaultdict(float)
new_edgelist = edgelist.copy()

g = Graph()
edge = edgelist
for e in edgelist:
    g.add_edge(e[0], e[1])
betweenness = sc.parallelize(user_list).flatMap(lambda s: g.bfs(s)).reduceByKey(lambda x,y: x+y).map(lambda s: (s[0], s[1]/2))\
    .sortBy(lambda s: (-s[1], s[0])).collect()

#print(betweenness)
def dfs(root):
    visited = [root]
    parent = defaultdict(set)
    treeLevel = defaultdict(set)
    value = defaultdict(float)
    parent[root] = None
    value[root] = 1
    treeLevel[root] = 0
    weight = defaultdict(float)
    for v in dic_u[root]:
        visited.append(v)
        value[v] = 1
        parent[v] = [root]
        treeLevel[v] = 1
    i = 1
    while i <= len(visited)-1:
        node = visited[i]
        weight[node] = 1
        link_node = dic_u[node]
        value_i = 0
        for v in parent[node]:
            value_i += value[v]
        value[node] = value_i
        for v in link_node:
            if v not in visited:
                visited.append(v)
                treeLevel[v] = treeLevel[node] + 1
                parent[v] = [node]
            else:
                if treeLevel[v] == treeLevel[node] + 1:
                    parent[v].append(node)
        i += 1
    visited_r = list(reversed(visited))
    for v_1 in visited_r[:-1]:
        for v_2 in parent[v_1]:
            k = tuple(sorted([v_1, v_2]))
            v = weight[v_1] * (value[v_2] / value[v_1])
            weight[v_2] += v
            yield (k, v)
##
def find_c(u_l):
    node_v = []
    cmnts = []
    for root in u_l:
        if root not in node_v:
            visited = [root]
            i = 0
            while i <= len(visited) - 1:
                node = visited[i]
                link_node = dic_u[node]
                for v in link_node:
                    if v not in visited:
                        visited.append(v)
                i += 1
            visited.sort()
            node_v = node_v + visited
            cmnts.append(visited)

    return cmnts

betweenness = sc.parallelize(user_list).flatMap(lambda s: dfs(s)).reduceByKey(lambda x,y: x+y).map(lambda s: (s[0], s[1]/2))\
    .sortBy(lambda s: (-s[1], s[0])).collect()

result_1 = betweenness

#############################

m = len(betweenness)
inl_dicu= dic_u
max_mdlrt = 0
new_btns = betweenness
best_cmnt = ['hhhhh']
c = 0
while c < m:
    mdlrt = 0
    community = find_c(user_list)
    for cmnt in community:
        q = 0
        for i in cmnt:
            for j in cmnt:
                if i != j:
                    k_i = len(inl_dicu[i])
                    k_j = len(inl_dicu[j])
                    if j in inl_dicu[i]:
                        a = 1
                    else:
                        a = 0
                    q0 = (a - (k_i * k_j / (2 * m))) / (2 * m)
                    q += q0
        mdlrt += q
    if mdlrt > max_mdlrt:
        max_mdlrt = mdlrt
        best_cmnt = community.copy()
    #find the new btwness to find the most community
    new_btns = sc.parallelize(user_list).flatMap(lambda s: dfs(s)).reduceByKey(lambda x, y: x + y) \
        .map(lambda s: (s[1]/2, [s[0]])).reduceByKey(lambda x,y: x+y).sortBy(lambda s: (-s[0])).first()
    cuts = new_btns[1]
    for cut in cuts:
        s = cut[0]
        t = cut[1]
        dic_u[s].remove(t)
        dic_u[t].remove(s)
    c += len(cuts)



max_modularity_communitites = sc.parallelize(best_cmnt).sortBy(lambda s: (len(s), s)).collect()

#print reselt
c= sc.parallelize(result_1).sortByKey(lambda x: x[0]).sortBy(lambda x: x[1],False)
text = open(output_1, "w")
if c:
    for e in c.collect():
        text.write("(")
        text.write("\'")
        text.write(str(e[0][0]))
        text.write("\'")
        text.write(", ")
        text.write("\'")
        text.write(str(e[0][1]))
        text.write("\'")
        text.write(")")
        text.write(", ")
        text.write(str(e[1]))
        text.write("\n")
text.close()

text2 = open(output_2, 'w')
for i in max_modularity_communitites:
    text2.write(str(i).strip('[').strip(']') + '\n')
text2.close()
