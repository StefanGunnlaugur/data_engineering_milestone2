import numpy as np
import math
from scipy import stats


Xs= [[9,7,6,5,7,8,9,7,8,6],[7,6,5,4,3,2,3,4,5,6], [7,6,8,9,7,6,5,4,3,4], [2,3,4,3,6,4,5,3,4,2], [8,7,6,7,8,9,8,9,7,8], \
   [1,6,2,4,3,5,6,7,8,6], [1,2,3,4,2,4,2,5,6,5], [9,7,6,5,7,8,9,7,8,6]]

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

print(total_correlation(Xs))