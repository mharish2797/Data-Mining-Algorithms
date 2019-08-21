from itertools import combinations
import sys
class apriori:
    
    '''variables'''
    mapper={}
    prev_freq_itemsets=[]
    support=3
    k_value=2
    basket=[]
    cluster={}
    len_prev_freq_itemset=0
    
    '''methods'''
    def __init__(self,fname,k,support):
        f=open(fname,"r")
        mapper={}
        for i in f:
            line=i.strip().split(",")
            b,a=line[0],line[1]
            if a in mapper and b not in mapper[a]:
                mapper[a].append(b)  
            else:
                mapper[a]=[b]
        for i in mapper:
            mapper[i]=sorted(mapper[i])
        self.basket= list(mapper.items())
        self.k_value=k
        self.support=support        
           
    def freq_elements(self,element):
            if self.mapper[element]>=self.support:
                return True
            else:
                return False
            
    def assign_map_value(self,element):
        self.mapper[element]=1
#        self.freq_updater(element)
    
    def update_map_value(self,element):
        self.mapper[element]+=1
#        self.freq_updater(element)
        
    def is_candidate(self,element,k):
        subsets=list(combinations(element,k))
        for subset in subsets:
            if subset not in self.prev_freq_itemsets:
                return False
        return True
              
    def get_k_itemset_pairs(self,liz,x):
        gen_combinations=list(combinations(liz,x))
        for element in gen_combinations:
            if element in self.mapper:
                self.update_map_value(element)
            elif self.len_prev_freq_itemset== 0 or self.is_candidate(element,x-1): 
                self.assign_map_value(element)
                     
    def generate_freq(self,element):
        count,name=self.mapper[element]
        if count>=self.support:
            self.cluster[element]=name
    
    def update_kmap_value(self,name,element):
        x,y=self.mapper[element]
        y.append(name)
        self.mapper[element]=(x+1,y)
        self.generate_freq(element)
    
    def assign_kmap_value(self,name,element):
        self.mapper[element]=(1,[name])
        self.generate_freq(element)
        
    def get_kth_itemsets(self,basket):
        name,liz=basket
        gen_combinations=list(combinations(liz,self.k_value))
        for element in gen_combinations:
            if element in self.mapper:
                self.update_kmap_value(name,element)
            elif self.len_prev_freq_itemset== 0 or self.is_candidate(element,self.k_value-1): 
                self.assign_kmap_value(name,element)

    def refresh_values(self):
        self.prev_freq_itemsets= list(filter(self.freq_elements,self.mapper))
        self.mapper={}
        self.len_prev_freq_itemset=len(self.prev_freq_itemsets)
        
    def printer(self):
        if len(self.cluster)==0:
            print("No Subraph of given size exists")
            return
        
        for left_node in self.cluster:
            combos=list(combinations(self.cluster[left_node],self.support))
            for right_node in combos:
               print("{"+','.join(sorted([str(x) for x in left_node]))+"}{"+','.join(sorted([str(x) for x in right_node]))+"}")

        
    def run_apriori(self):        
        for k in range(1,self.k_value):
            for (name,list_elements) in self.basket:
                self.get_k_itemset_pairs(list_elements,k)
            self.refresh_values()
            if(self.len_prev_freq_itemset==0):
                print("No Subraph of given size exists")
                return
            
        for bask in self.basket:
            self.get_kth_itemsets(bask)
            
        self.printer()


input_text=sys.argv[1]
k_value=int(sys.argv[2])
support=int(sys.argv[3])

#input_text='test2.txt'
#k_value=2
#support=3

obj=apriori(input_text,k_value,support)
obj.run_apriori()


