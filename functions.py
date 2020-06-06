from correlation import total_correlation
import pickle
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Row

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name ):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def generate_data(spark, size):
    df = pd.read_pickle("data_milestone2_raw.pkl")

    dataframes = []
    for key in df.keys():
        dataframes.append(df[key])
    new_df = pd.concat(dataframes)
    sparkdf = spark.createDataFrame(new_df)

    clean_df = sparkdf.drop('highest price').drop('lowest price').drop('closing price').drop('volumes')
    dataframe_sp = clean_df.groupBy("name").agg(F.collect_list("opening price").alias("op. price"))

    subset_dataframe = spark.createDataFrame(dataframe_sp.take(2044))
    pandas_subset = dataframe_sp.take(2044)

    tuple_list = []
    for name, value in pandas_subset:
        tuple_list.append((name, value))

    return tuple_list

def get_data(size = 500):
    data = pd.read_pickle("data_milestone2.pkl")
    data_len = len(data)
    if size > data_len:
        print("Selected size too large, maximum of 2044 companies, giving all available data")
        return data
    if size < 1:
        print("Selected size too small, giving you 10 companies")
        return data[:10]

    return data[:size]

def subsets_leq_k(A,K):
    collection_subsets = [[] for i in range(K)]
    N = len(A)
 
    mask = 0
    while mask < (1<<N): 
        subset = []
        for n in range(N):
            if ((mask>>n)&1) == 1:
                subset.append(A[n])
        if len(subset) > 0:
            collection_subsets[len(subset)-1].append(subset)

        if K == 0:
            break
 
        if bin(mask).count("1") < K:
            mask += 1
        else:
            mask = (mask|(mask-1))+1

    return collection_subsets 


def subsets_eq_k(A,K):
    subsets = []
    N = len(A)

    mask = (1<<K)-1
    while mask < (1<<N):
        subset = []
        for n in range(N):
            if ((mask>>n)&1) == 1:
                subset.append(A[n])
 
        subsets.append(subset)
 
        if mask == 0:
            break
 
       
        a = mask & -mask                
        b = mask + a                   
        mask = int(((mask^b)>>2)/a) | b 

    return subsets


def reduce_mapping_pearson(x):
  def pearson(data1, data2):
    name1_1 = data1[1][0]
    name2_1 = data2[1][0]
    return (pearsonr(data1[1][1],data2[1][1]), name1_1 + " X " + name2_1)
  
  return [functools.reduce(pearson, group) for _, group in groupby(sorted(x), key=itemgetter(0))]