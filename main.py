import os
import pandas as pd
import numpy as np

#general spark imports and setup
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

#setup imports
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
from pyspark.ml.linalg import SparseVector, DenseVector
from scipy.stats.stats import pearsonr
import math
import time
from itertools import groupby
from operator import itemgetter
import functools 


#import functions
from functions import subsets_leq_k, subsets_eq_k, reduce_mapping_total, reduce_mapping_pearson, get_data, generate_data
from aggregation import calculate_max, calculate_min, calculate_average
from correlation import pearson, total_correlation
from calculations import milestone_2



if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "12g").getOrCreate()
    conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    data = get_data(10)
    correlations = [('pearson', pearson), ('total_correlation', total_correlation)]
    aggregation = [calculate_max, calculate_min, calculate_average]
    correlation = correlations[0]
    number_of_p = 3
    result = milestone_2(calculate_average, correlation[1], number_of_p, data, sc, correlation[0])



      
