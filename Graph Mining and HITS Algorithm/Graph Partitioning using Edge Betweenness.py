from __future__ import print_function
import networkx as nx
import networkx.algorithms.centrality.betweenness as btw
import sys

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

# filename="D:\EduDataMining-553\HW5\graph2.txt"
# k=3

G=parse_input(filename)
while len(list(nx.connected_component_subgraphs(G)))<k:
    get_betweenness=btw.edge_betweenness_centrality(G)
    get_sorted=sorted(get_betweenness.items(), key=lambda x: (x[1],-1*min(x[0][0],x[0][1]),-1*max(x[0][0],x[0][1])), reverse=True)
    edge_to_remove_a,edge_to_remove_b=get_sorted[0][0]
    G.remove_edge(edge_to_remove_a,edge_to_remove_b)

cc=list(nx.connected_component_subgraphs(G))
cc=sorted(cc,key=lambda x: min(list(x.nodes)))

for component in cc:
    ids=sorted(component.nodes)
    print(*ids, sep=",")
