
# 15 servers one instance
putFinished3 = [0, 47, 89, 128, 174, 222, 268, 323, 370, 412, 456, 502, 536, 574, 611]
# 15 servers, two instance
putFinished4 = [0, 46, 103, 152, 201, 255, 306, 360, 415, 463, 514, 555, 607, 662, 717]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished3, label = "15 servers on one instance")
plt.plot(putFinished4, label = "15 servers on two instance")
# plt.plot(putFinished3, label = "15 servers")
plt.xlabel("time in second")
plt.ylabel("put request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()



