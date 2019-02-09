#!/usr/bin/env python2.7

#INF 553 HW5
#Myungjin Lee
#USCID: 5128876730

import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from collections import OrderedDict
import numpy
import sys
import re
import locale
import findspark
import time

def parse(x):
    return([str(s) for s in re.findall(r'\b[a-z]\b',x)])

def readfile(f_object):
    global tran_matx
    global node_dict
    global pair_dict
    global invr_node_dict

    data=input_file.map(parse).collect()
    count = 0
    for line in data:
        pair_dict[count] = line
        count += 1
    node_list = re.findall(r'\b[a-z]\b',str(pair_dict.values()))
    node_list = sorted(set(node_list))
    for index in range(len(node_list)):
        node_dict[node_list[index]] = index
    invr_node_dict = {v: k for k, v in node_dict.items()}
    tran_matx = numpy.zeros((len(node_list),len(node_list)))
    for pair in pair_dict.itervalues():
        tran_matx[node_dict[pair[0]]][node_dict[pair[1]]] = 1
        tran_matx[node_dict[pair[1]]][node_dict[pair[0]]] = 1
    
def graph(root):
    above_curr_nodes = OrderedDict()
    curr_nodes = OrderedDict()
    child_nodes = OrderedDict()
    
    curr_nodes[root] = {}
    curr_nodes[root]['child_edge'] = []
    curr_nodes[root]['num_parent'] = 0
    curr_nodes[root]['p_x'] = 1.0
    curr_nodes[root]['node_score'] = 0.0

    while(len(curr_nodes)>0):
        for index, node in curr_nodes.iteritems():
            rows = tran_matx[node_dict[index]]
            for conn_indx,conn_edge in enumerate(rows):
                node_name = invr_node_dict[conn_indx]
                if(conn_edge==0):
                    continue 
                if(node_name in above_curr_nodes):
                    continue
                if(node_name in curr_nodes):
                    continue
                if(node_name in child_nodes):
                    tmp_child == child_nodes[node_name]
                    tmp_child[node_name]['num_parent'] =+ 1
                    p_xs = tmp_child[node_name]['p_x'] + tmp_child[node_name]['num_parent']
                    tmp_child[node_name]['p_x'] = p_xs
                    curr_nodes[index]['child_edge'].extend([(node_name, 0.0)])
                    continue
                #if (conn_edge == 1 and node_name not in above_curr_nodes and node_name not in curr_nodes): 
                tmp_child = {}
                tmp_child[node_name] = {}
                curr_nodes[index]['child_edge'].extend([(node_name, 0.0)])
                tmp_child[node_name]['child_edge'] = []
                tmp_child[node_name]['num_parent'] = 1
                tmp_child[node_name]['p_x'] = max(1.0, tmp_child[node_name]['num_parent'])
                tmp_child[node_name]['node_score'] = 0.0
                child_nodes.update(tmp_child)
        above_curr_nodes.update(curr_nodes)
        curr_nodes = child_nodes
        child_nodes = OrderedDict()
    return above_curr_nodes, root

def compute_score(all_nodes, root):
    curr_node_score = all_nodes[root]['node_score']
    curr_child_edge = all_nodes[root]['child_edge']
    curr_num_parent = all_nodes[root]['num_parent']
    if len(curr_child_edge)==0:
        all_nodes[root]['node_score'] = 1.0
        return all_nodes[root]['node_score'], all_nodes
    curr_node_score = max(1.0, curr_num_parent)
       
    accum = 0.0
    new_edges = []
    for edge in curr_child_edge:
        child_score = compute_score(all_nodes, edge[0])
        edge_score = curr_node_score / all_nodes[edge[0]]['p_x']
        edge_score = edge_score * child_score[0]
        new_edges.extend([(edge[0],edge_score)])
        accum += edge_score
    all_nodes[root]['child_edge'] = new_edges
    all_nodes[root]['node_score'] = accum + 1.0
    return all_nodes[root]['node_score'], all_nodes

def calculate_score(x):
    global score_matx
    for key, val in x.iteritems():
        for child in val['child_edge']:
            child_node, edge_score = child[0], child[1]
            i = node_dict[key]
            j = node_dict[child_node]
            score_matx[i][j] = score_matx[i][j] + edge_score
            score_matx[j][i] = score_matx[i][j]
    return score_matx


if __name__ == '__main__':

    sc=SparkContext(appName="inf553")
    spark=SparkSession.builder.appName("inf553").getOrCreate()

    input_file=sc.textFile(sys.argv[1])
    output_file=sys.argv[2]
    f_out_obj=open(output_file,'w')

    file_name = sys.argv[1]
    node_dict = {}
    pair_dict = OrderedDict()
    invr_node_dict = {}
    tran_matx = []
    
    readfile(input_file) 
    score_matx = numpy.zeros((len(tran_matx),len(tran_matx)))

    tmp = sc.parallelize(node_dict.keys(), len(node_dict.keys())).map(graph) 
    tmp1 = tmp.map(lambda x: compute_score(x[0], x[1])).map(lambda x: x[1])
    score_matx = tmp1.map(calculate_score).reduce(lambda x,y:x+y) 

    score_matx = score_matx / 2.0
    for pair in pair_dict.values():
        i = node_dict[str(pair[0])]
        j = node_dict[str(pair[1])]
        k = "({0}, {1}), {2}".format(pair[0], pair[1], score_matx[i][j])
        f_out_obj.write(k +'\n')


