# 100 key value size
putFinished1 = [0, 829, 2042, 3897, 6184, 8972, 9974]
# 1,000 key value size
putFinished2 = [0, 992, 2204, 3976, 6408, 9335, 9981]
# 10,000 key value size
putFinished3 = [0, 736, 1900, 3888, 6504, 9986]
# 100,000 key value size
putFinished4 = [0, 282, 778, 1446, 2254, 3244, 4330, 5399, 6599, 7525, 8674, 9884, 9998]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished1, label = "100 bytes kv size")
plt.plot(putFinished2, label = "1,000 bytes kv size")
plt.plot(putFinished3, label = "10,000 bytes kv size")
plt.plot(putFinished4, label = "100,000 bytes kv size")
plt.xlabel("time in second")
plt.ylabel("get request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()



