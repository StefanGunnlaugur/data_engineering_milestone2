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

#import functions
from functions import subsets_leq_k, subsets_eq_k, get_data, generate_data, pretty_print_results
from aggregation import calculate_max, calculate_min, calculate_average
from correlation import pearson, total_correlation
from calculations import milestone_calculations


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "29g").config("spark.driver.maxResultSize","2g").config("spark.executor.memory", "29g").getOrCreate()
    conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    take_input = True
    if take_input:
        nr_stocks = input('Choose number of stocks to compute correlations: ')
        data = get_data(int(nr_stocks))
        corr_idx = input('Choose correlation method, 0:Pearson - 1:Total: ')
        agg_idx = input('Choose aggregation method, 0:Max - 1:Min - 2:Avg - 3:Identity: ')
        correlations = [('pearson', pearson), ('total_correlation', total_correlation)]
        aggregation = [("Max", calculate_max), ("Min", calculate_min), ("Average", calculate_average), ("Identity", None)]
        agg = aggregation[int(agg_idx)]
        correlation = correlations[int(corr_idx)]
        p = input('Choose p>=3: ')
        number_of_p = int(p)
        milestone_calc = milestone_calculations()
        result = milestone_calc.compute_correlation(agg[1], agg[0], correlation[1], correlation[0], data, number_of_p, sc, spark)
        pretty_print_results(result)

    else:
        nr_stocks = 10
        p = 3
        corr_idx = 0
        agg_idx = 2
        data = get_data(int(nr_stocks))
        correlations = [('pearson', pearson), ('total_correlation', total_correlation)]
        aggregation = [("Max", calculate_max), ("Min", calculate_min), ("Average", calculate_average), ("Identity", None)]
        agg = aggregation[agg_idx]
        correlation = correlations[int(corr_idx)]
        number_of_p = int(p)
        milestone_calc = milestone_calculations()
        result = milestone_calc.compute_correlation(agg[1], agg[0], correlation[1], correlation[0], data, number_of_p, sc, spark)
        pretty_print_results(result)


      
