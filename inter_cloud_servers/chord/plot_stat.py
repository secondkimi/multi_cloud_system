
import matplotlib.pyplot as plt

if __name__ == "__main__":
	x = []
	with open('stats.txt') as f:
		content = f.readlines()
		for line in content:
			tokens = line.split(':')
			if tokens[0] == 'ID':
				x.append(int(tokens[2].strip()))

	fig, ax = plt.subplots()
	#x = [300, 127, 263, 782, 210, 769, 466, 2036, 395, 350, 922, 1827, 738, 264, 27, 524]
	#x_32 = [154, 715, 248, 461, 550, 245, 675, 689, 573, 295, 417, 16, 15, 92, 151, 459, 63, 268, 852, 159, 173, 159, 63, 193, 522, 6, 330, 360, 281, 282, 41, 490]

	x.sort()
	print(x)
	#n, bins, patches = ax.hist(x, 16, density=1)
	n, bins, patches = ax.hist(x, len(x))
	ax.set_xlabel('number of keys on each node')
	ax.set_ylabel('pdf')
	ax.set_title("histogram of {} server nodes, {} keys".format(len(x), sum(x)))
	plt.show()