#!/usr/bin/env python2.7

#INF 553 HW4
#Myungjin Lee
#USCID: 5128876730

import sys
import heapq
import numpy
import math
import re
import copy 
from itertools import combinations

def dist(x, y):
    x = numpy.array(x[:-1])
    y = numpy.array(y[:-1])
    distance = numpy.linalg.norm(x-y)
    return distance

def mean(xy):
    keylist = re.findall('\d+',str(xy))
    keylist = map(int, keylist)
    cand = []
    for key in keylist:
        tmp = bak[key]
        cand.append(numpy.array(tmp[:-1]))
    average = numpy.mean(cand, axis = 0) 
    average = average.tolist()
    average.extend([""])
    return average
    
if __name__ == "__main__":

    slength = []
    swidth = []
    plength = []
    pwidth = []
    cname = []

    for line in open(sys.argv[1]):
        line = line.strip().split(',')
        slength.append(float(line[0]))
        swidth.append(float(line[1]))
        plength.append(float(line[2]))
        pwidth.append(float(line[3]))
        cname.append(str(line[4]))
    k = int(sys.argv[2])
   
    input_dict = {}
    for i, (sl, sw, pl, pw, cn) in enumerate(zip(slength, swidth, plength, pwidth, cname)):
        input_dict[i]=[sl, sw, pl, pw, cn]
   
    bak = copy.copy(input_dict)
    n = 0
    actual_total = set()
    while n < len(set(cname)):
        tmp2 = []
        for key, val in input_dict.iteritems():
            init = sorted(list(set(cname)))[n]
            if init == input_dict[key][4]:
                tmp2.append(key)
        actual_total.update(sorted(set(combinations(tmp2,2))))
        n += 1
    
    num_cluster = len(input_dict)
    dist_dict = {}
    while(num_cluster > k):
        key_list = input_dict.keys()
        for i in range(len(key_list)-1):
            for j in range(i+1, len(key_list)):
                distxy = dist(input_dict[key_list[i]], input_dict[key_list[j]])    
                if distxy not in dist_dict.values():
                    tmp = tuple([key_list[i]]) + tuple([key_list[j]])
                    dist_dict[tmp] = distxy
        dist_list = dist_dict.values()
        heapq.heapify(dist_list)
        clust_dist = heapq.heappop(dist_list)
        clust_x = None
        clust_y = None
        for key, val in dist_dict.iteritems():
            if val == clust_dist:
                clustxy = key
                clust_x = key[0]
                clust_y = key[1]
        if clust_x in input_dict and clust_y in input_dict:
            cent_clust = mean(clustxy)
            input_dict[clust_x, clust_y] = cent_clust 
            del dist_dict[clust_x, clust_y]
            del input_dict[clust_x]
            del input_dict[clust_y]
        else:
            del dist_dict[clust_x, clust_y]
        num_cluster = len(input_dict)

    predicted_total = set() 
    clusters = input_dict.keys()
    for i in range(len(clusters)):
        tmp1 = str(clusters[i])
        tmp2 = re.findall('\d+', tmp1)
        tmp2 = map(int, tmp2)
        tmp2 = sorted(tmp2)
        predicted_total.update(set(combinations(tmp2, 2)))
        line1 = 'Cluster'+str(i+1)+':'
        line2 = str(sorted(tmp2))
        print(line1+line2)
    true_pos = set(predicted_total).intersection(set(actual_total))
    print('Precision: '+str(float(len(true_pos))/float(len(predicted_total))))
    print('Recall: '+str(float(len(true_pos))/float(len(actual_total))))
