# Shooth function: moving average of n values
import numpy as np
import matplotlib.pylab as plt

def smooth(a,WSZ):
    out0 = np.convolve(a,np.ones(WSZ,dtype=int),'valid')/WSZ    
    r = np.arange(1,WSZ-1,2)
    start = np.cumsum(a[:WSZ-1])[::2]/r
    stop = (np.cumsum(a[:-WSZ:-1])[::2]/r)[::-1]
    return np.concatenate((  start , out0, stop  ))

A = [1, 2, 4, 3, 5, 4, 7, 8, 7, 9, 2, 5, 50, 23, 34, 12, 43, 56, 65, 66]
B = smooth(A, 3)
C = smooth(A, 5)
D = smooth(A, 7)

plt.plot(A,  label='raw')
plt.plot(B,  label='sm-3')
plt.plot(C,  label='sm-5')
plt.plot(D,  label='sm-7')
plt.legend()
plt.show()
