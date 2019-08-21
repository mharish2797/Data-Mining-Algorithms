from pyspark import SparkConf, SparkContext
import sys

sc=SparkContext("local","Lappname")

filename=sys.argv[1]
n=int(sys.argv[2])
j=int(sys.argv[3])
#
# filename="D:\EduDataMining-553\HW5\graph1.txt"
# n=5
# j=4

fileparse=sc.textFile(filename,3)
def row_parse_input(iterator):
    for filerecord in iterator:
        edge=filerecord.strip().split("\t")
        yield(int(edge[0])-1,int(edge[1])-1)

def col_parse_input(iterator):
    for filerecord in iterator:
        edge=filerecord.strip().split("\t")
        yield(int(edge[1])-1,int(edge[0])-1)

def printer(list_p,string):
    print "\t",string
    for i,val in enumerate(list_p):
        print "\t\t",i+1,round(val,2)

rowparse=fileparse.mapPartitions(row_parse_input).groupByKey().mapValues(lambda x:tuple(x))
colparse=fileparse.mapPartitions(col_parse_input).groupByKey().mapValues(lambda x:tuple(x))
hubs=[1 for i in range(n)]

def multiplier(row,right):
    sum=0
    for val in row:
        sum+=right[val]
    return sum

def normalize(input,n):
    result=[0 for i in range(n)]
    for a,b in input:
        result[a]=b
    maxi=max(result)
    return [float(x)/maxi for x in result]


printer_dict=[]
for i in range(j):
    # print "Iteration: ",i+1
    auth_rdd=colparse.mapValues(lambda x:multiplier(x,hubs)).collect()
    auth=normalize(auth_rdd,n)
    # printer(auth,"Authorities:")

    hub_rdd=rowparse.mapValues(lambda x:multiplier(x,auth)).collect()
    hubs=normalize(hub_rdd,n)
    # printer(hubs, "Hubs:")
    printer_dict.append((auth,hubs))

for i,val in enumerate(printer_dict):
    print "Iteration:",i+1
    a,h=val
    printer(a,"Authorities:")
    printer(h, "Hubs:")