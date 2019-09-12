import os
import sys
import time
import socket

def generate_server_config(num_servers = 3):
    ''' 
	generate a server config namly serverConfig_{n}.txt
    '''
    config_file = "{}_serverConfig.txt".format(num_servers)

    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)


    with open(config_file, "wt") as f:
        f.write("{}\n".format(num_servers))
	for i in range(0, num_servers):
            if i < 10:
                 write_str = "{}:450{}\n".format(str(IPAddr), i)
            else:
                 write_str ="{}:45{}\n".format(str(IPAddr), i)
            f.write(write_str)
	write_str = "max_num_threads 50\nelection_timeout 1000\nrpc_timeout 3000\nheartbeat_interval 18"
	f.write(write_str)
	f.close()

if __name__ == "__main__":
    arg_len = len(sys.argv)
    n = 3
    if arg_len == 2:
    	n = int(sys.argv[1])

    generate_server_config(num_servers = n)
