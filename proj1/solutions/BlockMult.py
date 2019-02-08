#!/usr/bin/env python2.7

# INF553 HW1
# Myungjin Lee
# USCID 5128876730

import locale
import findspark
findspark.init()
from pyspark import SparkContext
import sys
from pyspark.sql import SparkSession
import re

output_file='output.txt'
f_out_obj=open(output_file,'w')

sc=SparkContext(appName="inf553")
spark=SparkSession.builder.appName("inf553").getOrCreate()

fileA=sc.textFile(sys.argv[1])
fileB=sc.textFile(sys.argv[2])

def parse(x):
    return([int(s) for s in re.findall('\d+',x)])

def first(x):
   if x[2]!=1 or x[3]!=1:         
      x.insert(2,1)
      x.insert(3,1)
      x.insert(4,0)
   return(x)

def second(x):
   if len(x)>5:
      if x[5]!=1 or x[6]!=2:
         x.insert(5,1)
         x.insert(6,2)
         x.insert(7,0)   
   else:
      x.append(int(1))
      x.append(int(2))
      x.append(int(0))
   return(x)

def third(x):
   if len(x)>8:
      if x[8]!=2 or x[9]!=1:
         x.insert(8,2)
         x.insert(9,1)
         x.insert(10,0)
   else:
      x.append(int(2))
      x.append(int(1))
      x.append(int(0))
   return(x)

def fourth(x):
   if len(x)>11:
      if x[11]!=2 or x[12]!=2:
         x.insert(11,2)
         x.insert(12,2)
         x.insert(13,0)
   else:
      x.append(int(2))
      x.append(int(2))
      x.append(int(0))
   return(x)


def multiply(x):
    a=[]
    b=[]
    result=[]
    for i in x[1]:
        if i[0]=='A':
           a.append(i)
        else:
           b.append(i)
    for i in a:
        for j in b:
            if i[1]==j[1]:
               mult11=i[2][0]*j[2][0]+i[2][1]*j[2][2]
               mult12=i[2][0]*j[2][1]+i[2][1]*j[2][3]
               mult21=i[2][2]*j[2][0]+i[2][3]*j[2][2]
               mult22=i[2][2]*j[2][1]+i[2][3]*j[2][3]
               result.append(((mult11), (mult12), (mult21), (mult22)))
    return (x[0],result) 

def summ(x):
    return(x[0],map(sum, zip(*x[1])))
    
def modify(x): 
    result=[]
    count=0
    if len(x[1])==4:
        for i in range(2):
            for j in range(2):
                if x[1][count]!=0:
                   index1=i+1
                   index2=j+1
                   result.append((index1,index2,x[1][count]))
                count+=1
        return(x[0],result)
    else:
        pass

matA=fileA.map(parse)\
          .map(first)\
          .map(second)\
          .map(third)\
          .map(fourth)\
          .flatMap(lambda x:[((x[0], b+1), ('A', x[1], [x[2:14][2],x[2:14][5],x[2:14][8],x[2:14][11]])) for b in range(3)])

matB=fileB.map(parse)\
          .map(first)\
          .map(second)\
          .map(third)\
          .map(fourth)\
          .flatMap(lambda x:[((b+1, x[1]), ('B', x[0], [x[2:14][2],x[2:14][5],x[2:14][8],x[2:14][11]])) for b in range(3)])

output=matA.union(matB).groupByKey().map(lambda tup: (tup[0], [x for x in tup[1]])).map(multiply).map(summ).sortByKey().map(modify).collect()

for i in range(len(output)):
    if output[i]!=None:
       line=str(output[i])+'\n'
       f_out_obj.write(line[1:len(line)-2]+'\n')
