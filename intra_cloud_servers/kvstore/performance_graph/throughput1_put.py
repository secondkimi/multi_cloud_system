# 5 servers, one instance
putFinished1 = [0, 32, 87, 137, 194, 247, 301, 357, 414, 469, 522, 568, 623, 680, 728, 783]
# 9 servers, one instance
putFinished2 = [0, 0, 30, 76, 126, 176, 226, 226, 279, 327, 382, 433, 486, 540, 593, 645, 695, 748]
# 15 servers one instance
putFinished3 = [0, 47, 89, 128, 174, 222, 268, 323, 370, 412, 456, 502, 536, 574, 611]
# 15 servers, two instance
putFinished4 = [0, 46, 103, 152, 201, 255, 306, 360, 415, 463, 514, 555, 607, 662, 717]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished1, label = "5 servers")
plt.plot(putFinished2, label = "9 servers")
plt.plot(putFinished3, label = "15 servers")
plt.xlabel("time in second")
plt.ylabel("put request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()



