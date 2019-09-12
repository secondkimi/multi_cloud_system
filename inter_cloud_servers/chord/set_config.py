port = 8000

file_obj = open("serverConfig.conf", "w")
import sys

if len(sys.argv) != 2:
	raise Exception("should run as this format: python set_config.py <num_of_servers(int)>")

num = int(sys.argv[1])

file_obj.write(sys.argv[1]+"\r\n")

for i in range(num):
	p = port + i
	if p == 8888:
	    continue
	file_obj.write("127.0.0.1:{}".format(p)+"\r\n")

p = port + num
if p >= 8888:
    file_obj.write("127.0.0.1:{}".format(p)+"\r\n")

file_obj.close()


