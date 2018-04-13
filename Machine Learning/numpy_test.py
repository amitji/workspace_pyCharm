import numpy as np
import time


def test_run():
    t1 = time.time()
    #print np.array([(2,3,4), (5,6,7)])
    print np.empty((5,4))
    print np.ones((5,4), dtype=np.int_)
    a=np.random.rand(5,4,3)
    print a.shape[0]
    print a.size

    a=np.random.randint(0,10, size=(5,4))
    print "Array:\n", a
    print "Sum of each column:\n", a.sum(axis=0)
    print "Sum of each row:\n", a.sum(axis=1)
    print "Min of each column:\n", a.min(axis=0)
    print "Max of each row:\n", a.max(axis=1)
    print "mean of all elements:\n", a.mean()

    t2 = time.time()
    print "Time taken - : ", t2-t1, " seconds"

    a[0,:] = 2
    print a
    a[:,3] = [1,2,3,4,5]
    print a

    a= np.random.rand(5)
    indices = np.array([1,1,2,3])
    print a[indices]

    mean = a.mean()
    print mean
    #masking
    a[a<mean] = mean
    print a


    #https://machinelearningmastery.com/index-slice-reshape-numpy-arrays-machine-learning-python/

    # simple slicing
    from numpy import array
    # define array
    data = array([11, 22, 33, 44, 55])
    print(data[0:-1])

    data = array([[11, 22, 33],
    		[44, 55, 66],
    		[77, 88, 99]])
    # separate data
    X, y = data[:, :-1], data[:, -1]
    print(X)
    print(y)

    #Data Shape
    data = array([11, 22, 33, 44, 55])
    print(data.shape)

    data = data.reshape((data.shape[0], 1))
    print(data.shape)
    print (data)

    data = array([[11, 22, 33],
    		[44, 55, 66],
    		[77, 88, 99]])
    print(data.shape)
















if __name__ == "__main__":
    test_run()
