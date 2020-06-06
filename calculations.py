from functions import subsets_leq_k, subsets_eq_k
import time 
from itertools import groupby
from operator import itemgetter
import functools 
from scipy.stats.stats import pearsonr
import sys, math
#import total_correlation from correlation

class milestone_calculations:
    def __init__(self):
        self.array_dict = {}
        self.totalCorrelation = None

    def reduce_mapping_pearson(self, x):
        def pearson(data1, data2):
            name1_1 = data1[1]
            name2_1 = data2[1]
            value1 = self.array_dict[data1[0]][0]
            value2 = self.array_dict[data1[0]][1]
            
            return (name1_1 + " X " + name2_1, pearsonr(value1,value2))
        return [functools.reduce(pearson, group) for _, group in groupby(sorted(x), key=itemgetter(0))]

    def reduce_mapping_total(self, ind_to_map):
        def correlation(companies):
            lists = [el[1] for el in self.array_dict[companies]]
            names = [el[0] for el in self.array_dict[companies]]
            correlation = self.total_correlation(lists)
            name = ' + '.join(names)
            res = (name, correlation)
            return res
            
        res = list(map(correlation, ind_to_map))
        return res

    def milestone_2(self, aggregation, correlation, correlation_method, data, p, sc, spark):
        print()
        if correlation_method == 'total_correlation':
            self.total_correlation = correlation
            print("Creating pairs...")
            subsets = subsets_eq_k(data,p)
            partition = int(len(subsets) / 5)
            print("Pairs created....moving on....")
            all_combinations = [[] for i in range(partition)]
            for i in range(len(subsets)):
                self.array_dict[i] = subsets[i]
                index = i % partition
                all_combinations[index].append(i)

            print("Distributing to workers and reducing pairs...")
            start = time.time()
            res = sc.parallelize(all_combinations)
            res = res.flatMap(lambda x: self.reduce_mapping_total(x)).filter(lambda line: abs(line[1]) >= 0.1).sortBy(lambda line: -line[1]).take(10)
            end = time.time()
            print("Time elapsed --> {}sek".format(round(end-start, 3)))
            print(res)
            return res
        
        elif correlation_method == 'pearson':
            subsets = subsets_leq_k(data,p-1)
            pair_averages = [subsets[0]]
            print("Creating pairs...")
            for i in range(1, len(subsets)):
                n = math.ceil(sys.getsizeof(subsets[i]) / 1024)
                #temp = sc.parallelize(spark.createDataFrame(subsets[i]).rdd.flatMap(lambda x: (aggregation(x))).collect(),2).collect()
                temp = sc.parallelize(subsets[i]).flatMap(aggregation).collect()
                pair_averages.append(temp)
            partition = int(len(subsets[0]) * len(subsets[1]) / 5)

            print("Pairs created... moving on...")
            t = 0
            all_combinations = [[] for i in range(partition)]
            for x in range(math.floor(p/2)):
                for s in range(p):
                    length_in_subset_1 = x+1
                    length_in_subset_2 = s+1
                    if (length_in_subset_1 + length_in_subset_2) == p:
                        for a in range(len(pair_averages[x])):
                            for b in range(len(pair_averages[s])):
                                names1 = pair_averages[s][b][0][0].split("->")
                                names2 = pair_averages[x][a][0][0].split("->")
                                if not any((True for x in names1 if x in names2)):
                                    index = t % partition
                                    self.array_dict[t] = [pair_averages[s][b][0][1], pair_averages[x][a][0][1]]
                                    tmp1 = (t, pair_averages[s][b][0][0])
                                    tmp2 = (t, pair_averages[x][a][0][0])
                                    all_combinations[index].append(tmp1)
                                    all_combinations[index].append(tmp2)
                                    t=t+1
                

            print("Distributing to workers and reducing pairs...")
            start = time.time()
            n = math.ceil(sys.getsizeof(all_combinations) / 1024)
            res = sc.parallelize(all_combinations)
            res = res.flatMap(lambda x: self.reduce_mapping_pearson(x)).filter(lambda line: abs(line[1][0]) >= 0.9).sortBy(lambda line: -line[1][0]).take(10)
            end = time.time()
            print("Time elapsed --> {}sek".format(round(end-start, 3)))
            print(res)
            return res
        else:
            print("Select a valid correlation method")