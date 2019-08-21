from pyspark import SparkContext
import sys
sc = SparkContext(appName="inf553")

def parse_input(x):
    l=x.strip().split(',')
    return (int(str(l[0])[1:]),[int(x) for x in l[1:]])


def movie_mapper(movie_list):
    map_movie = {}
    for i in movie_list:
        map_movie[i[0]] = i[1]
    return map_movie

def hash_func(x,i):
    return (5*x+13*i)%100

def sign_value(i,y):
    return min([hash_func(x, i) for x in y])

def simple_transform(x,y):
    result=[str(sign_value(i,y)) for i in range(20)]
    i=0
    a=[(0,(x,",".join(result[i:i+5])))]
    i+=5
    b=[(1,(x,",".join(result[i:i+5])))]
    i+=5
    c=[(2,(x,",".join(result[i:i+5])))]
    i+=5
    d=[(3,(x,",".join(result[i:i+5])))]
    return a+b+c+d

def in_band(iterator):
    mapper={}
    for x in iterator:
        sign_id, iter = x
    for user_sign_list in iter:
        user,user_sign=user_sign_list
        if user_sign not in mapper:
            mapper[user_sign]=[user]
        else:
            mapper[user_sign].append(user)
    for k in mapper:
        lis=mapper[k]
        for i in lis:
            yield (i,lis)

def jaccard(a,b,map_movie):
    lis_a=set(map_movie[a])
    lis_b=set(map_movie[b])
    return len(lis_a.intersection(lis_b))/float(len(lis_a.union(lis_b)))

def similarity(x,y,map_movie):
    result=[]
    for user in y:
        jac=jaccard(x,user,map_movie)
        result.append([user,jac])
    result.sort(key=lambda i:(-i[1],i[0]))
    new_res=[k[i] for i in range(1) for k in result[:5]]
    return new_res

def recommend(a,b,map_movie):
    x=similarity(a, remove_duplicate(a,b),map_movie)
    dict={}
    for i in x:
        temp=map_movie[i]
        for j in temp:
            if j in dict:
                dict[j]+=1
            else:
                dict[j]=1
    result=sorted(dict.items(),key=lambda y:(-y[1],int(y[0])))[:3]
    kk=[i[0] for i in result]
    return kk

def remove_duplicate(x,y):
    return [i for i in y if i != x]

def print_output(x,y):
    string=""
    if(len(y)>0):
        str_y=[str(u) for u in y]
        string='U' + str(x) + "," + ",".join(str_y)+"\n"
    return(string)

def main_func(input,output):
    user_movies = sc.textFile(input, 4).map(lambda x: parse_input(x))
    map_movie = movie_mapper(user_movies.collect())
    simple_matrix = user_movies.map(lambda (x, y): simple_transform(x, y)).coalesce(1)
    simple_transpose_matrix = simple_matrix.flatMap(lambda x: x).groupByKey().mapValues(lambda x:list(x))
    band_simple = simple_transpose_matrix.partitionBy(4).mapPartitions(lambda x:in_band(x)).aggregateByKey([], lambda x, y: list(set(x + y)),lambda x, y: list(set(x + y))) \
        .map(lambda (x, y): (x, recommend(x, y,map_movie)))
    output_result = band_simple.sortByKey().map(lambda (x, y): print_output(x, y)).collect()
    file = open(output, "w+")
    for i in output_result:
        if(i!=""):
            file.write(i)
    file.close()

        
input=sys.argv[1]
output=sys.argv[2]
main_func(input,output)


