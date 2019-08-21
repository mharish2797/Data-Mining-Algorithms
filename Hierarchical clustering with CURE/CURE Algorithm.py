import heapq
import math as m
from itertools import combinations as combo
from operator import add
import sys

GLOBAL_ID = 0
class point_obj:
    variables=[]
    id=0
    tag=""
    
    def __init__(self,id,input_point):
        self.id=id
        self.variables=input_point[:-1]
        self.tag=input_point[-1]
        
    def printer(self):
        print(self.id,self.variables,self.tag)
    
class heap:
    d=0
    a=None
    b=None
    def __init__(self,d,a,b):
        self.d=d
        self.a=a
        self.b=b
    
    def __lt__(self,other):
        return self.d<other.d
    
    def __str__(self):
        return self.__repr__()
    
    def __repr__(self):
        return str(self.a) + " " + str(self.b)
    
class cluster_class:
    centroid=[] #array of co-ords
    count=1
    points=[] #array of points
    sum_points=[] #array of co-ords
    representatives=[] #array of co-ords
    
    def __init__(self,points,count):
        global GLOBAL_ID
        self.id = GLOBAL_ID
        GLOBAL_ID += 1
        self.centroid=points.variables
        self.points=[points]
        self.count=count
        self.sum_points=points.variables
    
    def printer(self):
        print(self.centroid,self.count)
        for i in self.representatives:
            print(i)
        
    def dist(self,point_a,point_b):
        sum=0
        for i in range(len(point_a)):
            sum+=(point_a[i]-point_b[i])**2
        return m.sqrt(sum)
    
    def merge_cluster(self,cluster):
        self.points+=cluster.points
        self.count+=cluster.count
        self.sum_points=list(map(add,self.sum_points,cluster.sum_points))
        self.centroid=[i/self.count for i in self.sum_points]
        
    def add_points(self,point):
        self.points.append(point)
    
    def get_representatives(self):
        global global_n
        n=global_n
        if(len(self.points)<=n):
            self.representatives=[xx.variables for xx in self.points]
        else:
            while(len(self.representatives)<n):
                temp_dist=0
                candidate_point=[]
                for i in self.points:
                    new_dist=self.get_closest_point(i.variables)
                    if(new_dist>temp_dist):
                        temp_dist=new_dist
                        candidate_point=i
                self.representatives.append(candidate_point.variables)
                
    def get_closest_point(self,candidate):
        if len(self.representatives)==0:
            temp_dist=self.dist(candidate,self.centroid)
        else:
            temp_dist=self.dist(candidate,self.representatives[0])
            for i in self.representatives:
                new_dist=self.dist(candidate,i)
                if(new_dist<temp_dist):
                    temp_dist=new_dist
        return temp_dist
    
    def get_unit_vector(self,point):
        vector=[point[i]-self.centroid[i] for i in range(len(self.centroid))]
        vector_sumsq=m.sqrt(sum(x*x for x in vector))
        if(vector_sumsq>0): 
            unit_vector=[j/vector_sumsq for j in vector]
            return unit_vector
        else:
            return vector
       
    
    def new_move_representative(self):

        global global_alpha
        alpha=global_alpha
        new_representatives=[]
        for i in self.representatives:
            temp_rep=i
            for coord in range(len(temp_rep)):
                temp_x=temp_rep[coord]
                temp_rep[coord]=(1-alpha)*temp_x+alpha*self.centroid[coord]
            new_representatives.append(temp_rep)    
            
        self.representatives=new_representatives

    
    def move_representatives(self):
        global global_alpha
        alpha=global_alpha
        new_representatives=[]
        for i in self.representatives:
            new_dist=self.dist(self.centroid,i.variables)*alpha
            unit_vector=self.get_unit_vector(i.variables)
            new_vector=[unit_vector[jk]*new_dist+self.centroid[jk] for jk in range(len(self.centroid))]
            new_representatives.append(new_vector)    
            
        self.representatives=new_representatives
    
    def __str__(self):
        return self.__repr__()
    
    def __repr__(self):
        return str(self.id)
 

class sample_cluster:
    input_dataset=[] #array of points
    sample_points=[] #array of points
    training_map={} #map of tag:[indexes]
    actual_clusters=[]
    golden_pairs=[]
    k=0
    def __init__(self,filename,k):
        input_fil=open(filename,"r")
        self.k=k
        for i,val in enumerate(input_fil):
            temp=val.strip().split(",")
            for j in range(len(temp)-1):
                temp[j]=float(temp[j])
            new_point=point_obj(i,temp)
            self.input_dataset.append(new_point)
            if temp[-1] in self.training_map:
                self.training_map[temp[-1]].append(i)
            else:
                self.training_map[temp[-1]]=[i]
        self.get_golden_pairs()
                
    def get_sample_points(self,filename):
        sample_indicators=[]
        input_file=open(filename,"r")
        sample_indicators=[int(i.strip()) for i in input_file.readlines()]
        self.sample_points=[self.input_dataset[z] for z in sample_indicators]
        for point in self.sample_points:
            self.input_dataset.remove(point)
        
    def dist(self,point_a,point_b):
        sum=0
        for i in range(len(point_a)):
            sum+=(point_a[i]-point_b[i])**2
        return m.sqrt(sum)

    def generate_clusters(self):
        initial_clusters=[]
        heap_data=[]
        for point in self.sample_points:
            new_cluster=cluster_class(point,1)
            initial_clusters.append(new_cluster)
        
        possible_combos=list(combo(initial_clusters,2))
        for (x,y) in possible_combos:
            heap_data.append( heap(self.dist(x.centroid,y.centroid),x,y))
        self.hierarchy_cluster(heap_data,initial_clusters)

    def hierarchy_cluster(self,heap_data,cluster_set):
        if(len(cluster_set)<=self.k):
            return cluster_set;
        heapq.heapify(heap_data)

        for h in range(len(cluster_set)-self.k):

            take_top=heapq.heappop(heap_data)
            candidate_a=take_top.a
            candidate_b=take_top.b

            cluster_set.remove(candidate_a)

            cluster_set.remove(candidate_b)
            new_rem=[]
            for element in heap_data:
                if element.a==candidate_a or element.b==candidate_a or element.a==candidate_b or element.b==candidate_b:
                    new_rem.append(element)
            for i in new_rem:
                heap_data.remove(i)

            candidate_a.merge_cluster(candidate_b)
            
            heapq.heapify(heap_data)
            for el in cluster_set:
                heapq.heappush(heap_data,heap(self.dist(candidate_a.centroid,el.centroid),candidate_a,el)) 
            cluster_set.append(candidate_a)
            
        for cluster in cluster_set:
            cluster.representatives=[]
            cluster.get_representatives()
            cluster.new_move_representative()
            cluster.representatives.sort()

        for k in cluster_set:
            print("###"*10)
            k.printer()
        
        cluster_set.sort(key=lambda x: x.representatives)
        print("Sorted")
        for k in cluster_set:
            print("###"*10)
            k.printer()
            
        self.actual_clusters= cluster_set

    def augment_cluster(self):
        
        user_pairs=[]
        for point in self.input_dataset:
            temp=sys.float_info.max
            assignable=None
            for cluster in self.actual_clusters:
                for rep in cluster.representatives:
                    t_dist=self.dist(point.variables,rep)
                    if(t_dist<temp):
                        temp=t_dist
                        assignable=cluster
            assignable.add_points(point)
        
        printable_cluster=[]
        for i,cluster in enumerate(self.actual_clusters):
            temp_=[]
            for poin in cluster.points:
                temp_.append(poin.id)
            temp_.sort()
            user_pairs+=list(combo(temp_,2))
            printable_cluster.append(temp_)
        printable_cluster.sort()
        
        for i,val in enumerate(printable_cluster):
            print("Cluster",str(1+i)+":",val)
        
        self.get_estimates(set(user_pairs))
        
    def get_golden_pairs(self):
        for each in self.training_map:
            self.golden_pairs+=list(combo(self.training_map[each],2))
        self.golden_pairs=set(self.golden_pairs)
        
    def get_estimates(self,user_pairs):
        tp_=user_pairs.intersection(self.golden_pairs)
        fp_=user_pairs.difference(self.golden_pairs)
        fn_=self.golden_pairs.difference(user_pairs)
        precision=len(tp_)/(len(tp_)+len(fp_))
        recall=len(tp_)/(len(tp_)+len(fn_))
        print("Precision =",str(precision)+", recall =",recall)
        
              
                                                        
global_k=3
global_dataset="iris.dat"
global_sample="sample.dat"
global_n=4
global_alpha=0.2

#global_k=sys.argv[1]
#global_dataset=sys.argv[3]
#global_sample=sys.argv[2]
#global_n=sys.argv[4]
#global_alpha=sys.argv[5]

s= sample_cluster(global_dataset,global_k)
s.get_sample_points(global_sample)
s.generate_clusters()
s.augment_cluster()