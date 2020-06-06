from scipy.stats.stats import pearsonr
import numpy as np
import math
from scipy import stats
from sklearn import preprocessing
import pandas as pd
from dit import Distribution
from dit.multivariate import total_correlation as T

def pearson(data1, data2):
  name1_1 = data1[0]
  name2_1 = data2[0]
  return (pearsonr(data1[1],data2[1]), name1_1 + " X " + name2_1)


def total_correlation(Xs):
    pmf = []
    Xs = np.asarray(Xs)
    for stock in range(len(Xs)):
      s = pd.Series(Xs[stock])
      b = (s.groupby(s).transform('count') / len(s)).values
      pmf.append(b)
    pmf = np.asarray(pmf)
    pmf /= pmf.sum()
    custm = stats.rv_discrete(name='custm', values=(Xs, pmf))
    d = Distribution.from_ndarray(pmf)
    t = T(d)
    return t