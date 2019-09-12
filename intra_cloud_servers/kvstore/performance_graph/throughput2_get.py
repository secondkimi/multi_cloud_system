# 15 servers one instance
putFinished3 = [0, 114, 220, 381, 543, 783, 1033, 1317, 1663, 2057, 2593, 3099, 3652, 4266, 5005]
# 15 servers, two instance
putFinished4 = [0, 203, 560, 1077, 1608, 2250, 3044, 4002, 5130, 6697, 8292, 9706, 10000]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished3, label = "15 servers on one instance")
plt.plot(putFinished4, label = "15 servers on two instance")
# plt.plot(putFinished3, label = "15 servers")
plt.xlabel("time in second")
plt.ylabel("get request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()




