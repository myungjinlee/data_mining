#!/usr/bin/env python2.7

#INF 553 HW2
#Myungjin Lee
#USCID: 5128876730

# There is a bug in this program

import os
import sys
import re
import numpy
import time
import random
import math
from itertools import combinations
from collections import defaultdict

start=time.time()
f_inp_obj=open(sys.argv[1],'r')

def read_input(input_obj):
    buckets=[]
    for line in input_obj:
        if len(line)>1:
            bucket=[]
            bucket.append(re.findall('\d+',line))
            buckets.append(bucket)
    return buckets

def sampling(buckets,percentage,niter):
    prob=percentage/100.0
    buck_size=int(len(buckets))
    samp_size=int(math.ceil(prob*buck_size))
    random.seed(niter)
    samp_bucks=[]
    rdlist=[]
    rdnum=random.randint(0,buck_size-1)
    for i in range(samp_size):
        samp_bucks.append(buckets[rdnum])
        rdnum+=1
        if rdnum >= buck_size:
            rdnum-=buck_size
    return (samp_bucks,samp_size)

   
def trans_table(sampled_buckets):
    ttable=[]
    for bucket in sampled_buckets:
        for index in range(len(bucket[0])):
            ttable.append(bucket[0][index])  
    unique=sorted(set(ttable))
    return unique

def count_table(valtable, sampled_buckets, ttable):
    ctable={}
    if len(valtable) >0 :
        if type(valtable[0]) != tuple:
            for bucket in sampled_buckets:
                for item in bucket[0]:
                    if item in ttable:
                        ctable.setdefault(ttable.index(item),0)
                        ctable[ttable.index(item)]+=1
        else:
            for item in sampled_buckets:
                if item in valtable and item in ttable:
                    ctable.setdefault(ttable.index(item),0)
                    ctable[ttable.index(item)]+=1
                    #print(item, ctable[ttable.index(item)])
    else:
        pass
    return ctable

def freq_table(ctable,sratio,nbasket):
    ftable={}
    nt=math.ceil(sratio*nbasket)
    for key, val in ctable.iteritems():
        if val >= nt:
            ftable[key]=1
        else:
            ftable[key]=0
    return ftable

def first_pass(sampled_buckets, sratio, samp_size):
    trans = trans_table(bucks)
    count = count_table(sampled_buckets, sampled_buckets, trans)
    freq = freq_table(count, sratio, samp_size)
    return trans, freq

def glob_gen_pair(bucks, pairnum):
    gitem=[]
    gpair=[]
    for bucket in bucks:
        for item in bucket[0]:
            gitem.append(item)
    gitem=sorted(set(sorted(gitem)))
    gpair.append([list(combinations(gitem, pairnum))])
    return gpair
    
        
def samp_gen_pair(pre_ttable, pre_ftable, pairnum, negat):
    fitem=[]
    for key, val in pre_ftable.iteritems():
        if val==1:
            if type(pre_ttable[key]) != tuple:
               fitem.append(str(pre_ttable[key]))
            else:
               for i in range(len(pre_ttable[key])):
                   fitem.append(pre_ttable[key][i])
    fitem=sorted(set(fitem))
    fpair=list(combinations(fitem,pairnum))
    if len(negat[0])>0:
        for val in negat[0].itervalues():
            for item in fpair:
                if type(val)==tuple:
                    if all(i in item for i in val):
                        fpair.remove(item)
                else:
                    if val in item:
                        fpair.remove(item)
    return fpair

def samp_pair(sampled_buckets, pairnum):
    totalpair=[]
    tpair=[]
    for bucket in sampled_buckets:
        sitem=[]
        for item in bucket:
            for i in range(len(item)):
                sitem.append(item[i])
        totalpair.append(sorted(list(combinations(sorted(sitem),pairnum))))
    for item in totalpair:
        for i in range(len(item)):
            tpair.append(item[i])
    return tpair
 

def run_pass(sampled_buckets, pre_ttable, pre_ftable, sratio, samp_size, pair, negat):
    gpair=glob_gen_pair(bucks, pair)
    trans=trans_table(gpair)
    fspair=samp_pair(sampled_buckets,pair)
    if pre_ttable != None and pre_ftable != None:
        fpair=samp_gen_pair(pre_ttable, pre_ftable, pair, negat) #pre freq value pair 
    else:
        fpair=fspair
    count=count_table(fpair, fspair, trans)
    freq=freq_table(count, sratio, samp_size)
    return trans, freq


def negative_border(ttable, ftable, sratio, pair, fal, i, ni, fi):
    rem={}
    neg={}
    fre={}
    indicator=False
    if pair==1:
        global_chk=first_pass(bucks, sratio, len(bucks))
    else:
        global_chk=run_pass(bucks, None, None, sratio, len(bucks), pair, None)
    for key, val in ftable.iteritems():
        if val==0:
            print('Negative border items: ',ttable[key])
            neg[ni]=ttable[key]
            ni+=1
            rem[key]=ttable[key]
            if global_chk[1][key]==1:
                fal[i]=ttable[key]
                print('False', ttable[key])
                indicator=False
                i+=1
        else:
            fre[fi]=ttable[key]
            fi+=1
    if len(fal) ==0:
        indicator=True
    return indicator, rem, neg, fre, fal, i, ni, fi
            
def apriori(sampled_buckets, sratio, samp_size):
    flag=False
    numpass=1
    prev=first_pass(sampled_buckets, sratio, samp_size)
    frequent={}
    negative={}
    remove_item={}
    falsedict={}
    falindex=0
    negindex=0
    freindex=0
    while not flag:
        neg=negative_border(prev[0], prev[1], sratio, numpass, falsedict, falindex, negindex, freindex)
        remove_item[0]=neg[1]
        negative.update(neg[2])
        frequent.update(neg[3])
        falsedict.update(neg[4])
        falindex=neg[5]
        negindex=neg[6]
        freindex=neg[7]
        numpass+=1
        prev=run_pass(sampled_buckets, prev[0], prev[1], sratio, samp_size, numpass, remove_item)
        psum=0
        if 1 not in prev[1].values():
            flag=True
        for key, val in prev[1].iteritems():
            if val==1:
                psum+=numpass
        if psum < (numpass):
            flag=True
    negat=negative_border(prev[0], prev[1], sratio, numpass, falsedict, falindex, negindex, freindex)
    rflag=negat[0]
    negative.update(negat[2])
    frequent.update(negat[3])
    falsedict.update(negat[4])
    return rflag, negative, frequent, falsedict

def execute(total_buckets,sratio,totaliter):
    niter=0
    flag=False
    mypath="output/"
    if not os.path.exists(mypath):
        os.mkdir(os.path.dirname(mypath))
    while not flag:
        niter+=1
        out_file=mypath+"OutputForIteration_"+str(niter)+".txt"
        f_out_obj=open(out_file,'w')
        sample_bucks = sampling(bucks, samp_percent, niter)[0]
        sample_size = sampling(bucks, samp_percent, niter)[1]
        apr=apriori(sample_bucks, sratio, sample_size) 
        negative_dict=apr[1]
        frequent_dict=apr[2]
        false_negative=apr[3]
        newfalse=[]
        for item in false_negative.values():
            if type(item) != tuple:
                newfalse.append(int(item))
            else:
                newfalse.append(tuple(map(int,item)))
        print'False Negatives :', newfalse
        f_out_obj.write('Sampled created: ')
        line1=''
        for bucket in sample_bucks:
            for item in bucket:
                new=map(int, item)
                line1+=str(new)+','
        f_out_obj.write(line1[:-1])
        f_out_obj.write('\n'+'Frequent items: ')
        line2=''
        for val in frequent_dict.values():
            if type(val) != tuple:
                line2+=str("("+val+")")+','
            else:
                val=map(int, val)
                line2+=str(tuple(val))+','
        f_out_obj.write(line2[:-1])
        f_out_obj.write('\n'+'Negative border: ')
        line3=''
        for val in negative_dict.values():
            if type(val) != tuple:
                line3+=str("("+val+")")+','
            else:
                val=map(int, val)
                line3+=str(tuple(val))+','
        f_out_obj.write(line3[:-1])
        f_out_obj.close()
        flag=apr[0]
    print('----'+str(niter)+' times iteration ----')
if __name__ == '__main__':
    f_inp_obj=open(sys.argv[1],'r')
    #Variable
    samp_percent=80.0  #10
    supp_ratio=4/15.0 #4/15
    num_iter=1

    #Input
    bucks=read_input(f_inp_obj) #entire buckets
     
    #Run the program
    execute(bucks, supp_ratio, num_iter)    
    stop=time.time()
    print('----'+str(stop-start)+' seconds ----')
