from __future__ import print_function
import networkx as nx
import sys
import numpy as np
from numpy import linalg as la

filename=sys.argv[1]
k=int(sys.argv[2])

def parse_input(filename):
    edges=[]
    new_G=nx.Graph()
    fopen=open(filename,"r")
    fread=fopen.readlines()
    for line in fread:
        record=line.strip().split("\t")
        edges.append((int(record[0]),int(record[1])))
    new_G.add_edges_from(edges)
    return new_G

def next_component_to_partition(component_list):
    sorting=sorted(component_list,key=lambda x: (len(x.nodes),len(x.edges),-1*min(list(x.nodes))), reverse=True)
    return sorting[0]

def fiedel_graph(graph):
    node_list=list(graph.nodes)
    A=nx.adjacency_matrix(graph)
    D = np.diag(np.ravel(np.sum(A, axis=1)))
    L=D-A
    l,U=la.eigh(L)
    f=U[:,1]
    signs = np.ravel(np.sign(f))
    list_a=[]
    list_b=[]
    for i,val in enumerate(node_list):
        if signs[i]>0:
            list_a.append(val)
        else:
            list_b.append(val)
    edges_list=list(graph.edges)
    for edge_a,edge_b in edges_list:
        if (edge_a in list_a and edge_b in list_b) or (edge_b in list_a and edge_a in list_b):
            G.remove_edge(edge_a,edge_b)


# filename="D:\EduDataMining-553\HW5\graph2.txt"
# k=3
G=parse_input(filename)

while len(list(nx.connected_component_subgraphs(G)))<k:
    fiedel_graph(next_component_to_partition(list(nx.connected_component_subgraphs(G))))

cc=list(nx.connected_component_subgraphs(G))
cc=sorted(cc,key=lambda x: min(list(x.nodes)))

for component in cc:
    ids=sorted(component.nodes)
    print(*ids, sep=",")
