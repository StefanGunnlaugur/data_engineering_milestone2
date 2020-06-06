import os
import pandas as pd
import numpy as np

#general spark imports and setup
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf
from pyspark.sql.types import *

#setup imports
'''
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.ml.stat import Correlation
from pyspark.mllib.stat import Statistics
from pyspark.ml.linalg import SparseVector, DenseVector
from scipy.stats.stats import pearsonr
import math
import time
from itertools import groupby
from operator import itemgetter
import functools 
'''

#import functions
from functions import subsets_leq_k, subsets_eq_k, get_data, generate_data
from aggregation import calculate_max, calculate_min, calculate_average
from correlation import pearson, total_correlation
from calculations import milestone_calculations


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "12g").config("spark.executor.memory", "1g").getOrCreate()
    conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    #generate_data(spark)
    data = get_data(50)
    correlations = [('pearson', pearson), ('total_correlation', total_correlation)]
    aggregation = [calculate_max, calculate_min, calculate_average]
    correlation = correlations[0]
    number_of_p = 3
    milestone_calc = milestone_calculations()
    result = milestone_calc.milestone_2(calculate_average, correlation[1], correlation[0], data, number_of_p, sc, spark)

      
