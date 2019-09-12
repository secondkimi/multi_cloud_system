# 5 servers, one instance
putFinished1 = [0, 702, 1738, 3117, 4799, 7081, 9124, 9991]
# 9 servers, one instance
putFinished2 = [0, 199, 394, 672, 945, 1319, 1689, 2160, 2861, 3702, 4572, 5443, 6426, 7207, 7863]
# 15 servers one instance
putFinished3 = [0, 114, 220, 381, 543, 783, 1033, 1317, 1663, 2057, 2593, 3099, 3652, 4266, 5005]
# 15 servers, two instance
putFinished4 = [0, 203, 560, 1077, 1608, 2250, 3044, 4002, 5130, 6697, 8292, 9706, 10000]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished1, label = "5 servers")
plt.plot(putFinished2, label = "9 servers")
plt.plot(putFinished3, label = "15 servers")
plt.xlabel("time in second")
plt.ylabel("get request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()



