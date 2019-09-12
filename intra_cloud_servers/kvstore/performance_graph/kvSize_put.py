# 100 key value size
putFinished1 = [0, 53, 107, 161, 216, 270, 322, 377, 434, 486, 544, 599, 652, 705, 757]
# 1,000 key value size
putFinished2 = [0, 54,  106, 161, 212, 265, 318, 372, 429, 477, 530, 588, 638, 691, 744]
# 10,000 key value size
putFinished3 = [0, 51, 105, 156, 210, 261, 315, 364, 416, 465, 512, 556, 601, 639, 681]
# 100,000 key value size
putFinished4 = [0, 29, 50, 70, 80, 90, 100, 120, 130, 140, 158, 170, 180, 190, 200]


from matplotlib import pyplot as plt

plt.subplot(111)
plt.plot(putFinished1, label = "100 bytes kv size")
plt.plot(putFinished2, label = "1,000 bytes kv size")
plt.plot(putFinished3, label = "10,000 bytes kv size")
plt.plot(putFinished4, label = "100,000 bytes kv size")
plt.xlabel("time in second")
plt.ylabel("put request completed")
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
           ncol=2, mode="expand", borderaxespad=0.)
plt.show()



