from scipy.stats.stats import pearsonr
import numpy as np
import math
from scipy import stats

def pearson(data1, data2):
  name1_1 = data1[0]
  name2_1 = data2[0]
  return (pearsonr(data1[1],data2[1]), name1_1 + " X " + name2_1)


def total_correlation(Xs):
    pmf = []
    for stock in range(len(Xs)):
        a = stats.binom.pmf(Xs[stock], 10, 0.5)
        pmf.append(a)
    pmf = np.asarray(pmf) 

    numBins = 5
    b = []
    for stock in range(len(Xs)):
        jointProbs, edges = np.histogramdd(Xs[stock], bins=numBins)
        b.append(jointProbs)

    jointProbs /= jointProbs.sum()

    a = jointProbs * (np.log(jointProbs / np.prod(pmf)))
    total_correlation = a.sum()

    return total_correlation