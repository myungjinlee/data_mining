#!/usr/bin/env python2.7

# INF553 HW3
# Myungjin Lee
# USCID 5128876730

import locale
import findspark
findspark.init()
from pyspark import SparkContext
import sys
from pyspark.sql import SparkSession

#output_file=sys.argv[2]
#f_out_obj=open(output_file,'w')

def def_hash(inp_movies, i):
    hashed_movie=[]
    for row_ind in inp_movies:
        hashed_movie.append(minhash[row_ind][i])
    return hashed_movie

def min_sig(x):
    sig=0
    if len(x)!=0:
        sig=min(x)
    return sig

def banding(x):
    band=[]
    for i in range(0, len(x), 4):
        band.append(x[i:i+4])
    return band

def define_band(x):
    ans = []
    label = 0
    for i in x[1]:
        ans.append((str(label)+str(i), x[0]))
        label += 1
    return ans

def generate_upair(x):
    user_cand_pair = {}
    for (band, users) in x:
        for user in users:
            if user in user_cand_pair:
                user_cand_pair[user] |=set([i for i in users if i != user]) #union
            else:
                user_cand_pair[user] = set([i for i in users if i != user])
    return user_cand_pair.items()

def compute_jaccard(x, set_dict):
    user1 = x[0]
    user2_and_jac=[]
    for user2 in x[1]:
        x = len(set(set_dict[user1]).intersection(set(set_dict[user2])))
        x_sum_y = len(set(set_dict[user1]).union(set(set_dict[user2])))
        jac = float(x) / float(x_sum_y)
        user2_and_jac.append((user2, jac))
    return (user1, user2_and_jac)

def top5(candlist, num):
    result = []
    for i in range(min(num, len(candlist))):
        max_ind = i
        for k in range(i + 1, len(candlist)):
            if candlist[k][1] > candlist[max_ind][1]:
                max_ind = k
            elif candlist[k][1] == candlist[max_ind][1]:
                if int(candlist[k][0][1:]) < int(candlist[max_ind][0][1:]):
                   max_ind = k
        tmp = candlist[i]
        candlist[i] = candlist[max_ind]
        candlist[max_ind] = tmp
        result = sorted(candlist[:num], key = lambda x: int(x[0][1:]))
    return result

if __name__ == "__main__":

    sc=SparkContext(appName="inf553")
    spark=SparkSession.builder.appName("inf553").getOrCreate()

    input_file=sc.textFile(sys.argv[1])
    output_file=sys.argv[2]
    f_out_obj=open(output_file,'w')
    
    data=input_file.map(lambda x: (x.split(',')[0], x.split(',')[1:]))\
                   .map(lambda x: (x[0], [int(i) for i in x[1]]))

    user_movie_dict = {}
    for i in data.collect():
        user_movie_dict[i[0]] = i[1]

    # minhash
    movies=100
    signa=20
    minhash = {}
    for movie in range(movies):
        signa_array=[]
        for sindex in range(signa):
            hashed=(3*movie+13*sindex)%100
            signa_array.append(hashed)
        minhash[movie] = signa_array

    users=input_file.map(lambda x: x.split(',')[0]).collect()
    user_sig_dict={}

    # create user_sig_dict
    for i in range(signa): 
        user_sig_list = data.map(lambda row: (row[0], def_hash(row[1],i)))\
                            .map(lambda row: (row[0], min_sig(row[1])))\
                            .collect()
        for j in user_sig_list:
            user_sig_dict.setdefault(j[0],[])
            user_sig_dict[j[0]].append(j[1])

    # banding 5 bands 4 values each
    banded = sc.parallelize(user_sig_dict.items(),4)\
             .map(lambda x: (x[0], banding(x[1])))

    user_pair = banded.flatMap(define_band)\
                      .groupByKey()\
                      .map(lambda x: (x[0], list(x[1])))\
                      .filter(lambda x: len(x[1]) >= 2)\
                      .mapPartitions(generate_upair)\
                      .reduceByKey(lambda x,y: list(set(x).union(set(y))))
    # top 5 users
    user_jaccard = user_pair.map(lambda x: compute_jaccard(x, user_movie_dict))\
                       .mapValues(lambda x: top5(x, 5))\
                       .map(lambda x: (x[0], [i[0] for i in x[1]]))
    user_result = sorted(user_jaccard.collect(), key = lambda x: int(x[0][1:]))
    
    # top 3 movies
    for user in user_result:
        user_array = []
        #user_array.append(user[0])
        for i in range(len(user[1])):
            user_array.append(user[1][i])
        movie_set = {}
        for all_user in user_array:
            movies = user_movie_dict.get(all_user)
            for movie in movies:
                movie_set.setdefault(movie,[])
                movie_set[movie].append(all_user)
        new = sorted(movie_set, key = lambda x:(len(movie_set[x]),-x), reverse = True)
        line = str(user[0])
        count = 0
        for item in new:
            if count < 3:
               count+=1
               line+=','+str(item)
        f_out_obj.write(line+'\n')

