from functions import subsets_leq_k, subsets_eq_k

array_dict = {}

def reduce_mapping_pearson(x):
  def pearson(data1, data2):
    name1_1 = data1[1]
    name2_1 = data2[1]
    
    value1 = array_dict[data1[0]][0]
    value2 = array_dict[data1[0]][1]
    
    return (pearsonr(value1,value2), name1_1 + " X " + name2_1)
  return [functools.reduce(pearson, group) for _, group in groupby(sorted(x), key=itemgetter(0))]

def reduce_mapping_total(x):
    def correlation(companies):
        lists = [el[1] for el in companies]
        names = [el[0] for el in companies]
        correlation = total_correlation(lists)
        name = ' + '.join(names)
        res = (name, correlation)
        return res
    res = list(map(correlation, x))
    return res

def milestone_2(aggregation, correlation, p, data, sc, correlation_method):
    if correlation_method == 'total_correlation':
        subsets = subsets_eq_k(data,p)
        partition = int(len(subsets) / 5)
        print("Pairs created....moving on....")
        all_combinations = [[] for i in range(partition)]
        for i in range(len(subsets)):
            index = i % partition
            all_combinations[index].append(subsets[i])
            if i % 100000 == 0:
                print(i)
        print("We have arrived at the reduce part of this assignment, best regards. Me, Stefan,Arngrimur")
        start = time.time()
        print("a", reduce_mapping(all_combinations[0]))
        res = sc.parallelize(all_combinations).flatMap(lambda x: reduce_mapping_total(x)).collect()#.filter(lambda line: abs(line[1]) >= 0.9).collect()
        end = time.time()
        print("With partitioning --> ", end-start)
        print(res)
    
    elif correlation_method == 'pearson':
        subsets = subsets_leq_k(data,p-1)
        pair_averages = [subsets[0]]
        for i in range(1, len(subsets)):
            temp = sc.parallelize(spark.createDataFrame(subsets[i]).rdd.flatMap(lambda x: (aggregation(x))).collect()).collect()
            pair_averages.append(temp)

        partition = int(len(subsets[0]) * len(subsets[1]) / 5)
        print("Pairs created....moving on....")
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
                    array_dict[t] = [pair_averages[s][b][0][1], pair_averages[x][a][0][1]]
                    tmp1 = (t, pair_averages[s][b][0][0])
                    tmp2 = (t, pair_averages[x][a][0][0])
                    all_combinations[index].append(tmp1)
                    all_combinations[index].append(tmp2)
                    t=t+1

        print("We have arrived at the reduce part of this assignment, best regards. Me, Stefan,Arngrimur")
        start = time.time()
        #res = sc.parallelize(all_combinations).partitionBy(6, lambda k: int(k)).map(lambda x: reduce_mapping(x)).filter(lambda line: abs(line[0]) >= threshold).collect()#.collect()
        res = sc.parallelize(all_combinations)
        res = res.flatMap(lambda x: reduce_mapping_pearson(x)).filter(lambda line: abs(line[0][0]) >= 0.9).take(10)#.filter(lambda line: abs(line[0][0]) >= 0.7).reduce( lambda x,y: x + y )#.collect()
        end = time.time()
        print("With partitioning --> ", end-start)
        print(res)

milestone_2(calculate_average, pearson, 3, tuple_list, 100)
    else:
        print("Select a valid correlation method")