import numpy as np

def calculate_max(x):
    length = len(x)
    names = []
    lists = []
    for i in range(length):
        names.append(x[i][0])
        lists.append(x[i][1])
    max_list = list(map(max, *x))
    final_name = ""
    for name in names:
      if final_name == "":
        final_name = final_name + name
      else:
        final_name = final_name + "->" + name
    return [[(final_name,max_list)]]


def calculate_min(x):
    length = len(x)
    names = []
    lists = []
    for i in range(length):
        names.append(x[i][0])
        lists.append(x[i][1])
    min_list = list(map(min, *x))
    final_name = ""
    for name in names:
      if final_name == "":
        final_name = final_name + name
      else:
        final_name = final_name + "->" + name
    return [[(final_name,min_list)]]


def calculate_average(x):
    length = len(x)
    t = np.zeros(len(x[0][1]))
    names = []
    for i in range(length):
        t = np.add(t,x[i][1])
        names.append(x[i][0])
    t = t/length
    final_name = ""
    for name in names:
      if final_name == "":
        final_name = final_name + name
      else:
        final_name = final_name + "->" + name
    return [[(final_name,list(t))]]