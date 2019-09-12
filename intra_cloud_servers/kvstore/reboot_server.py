import os
import sys
import time
from run_server import start_server
from kill_server import kill_server

def reboot():
    ''' 
	If not specified in the argument, read serverConfig.txt and reboot server 0.
	Can also reboot multiple servers by listing all server IDs
    '''
    server_id = 0
    arg_len = len(sys.argv)
    if arg_len < 2:
	config_file = "serverConfig.txt"
    else:
    	config_file = sys.argv[1]

    f = open(config_file)
    num_of_servers = int(f.readline())
    f.close()

    if arg_len < 3 and server_id < num_of_servers:
    	kill_server(server_id)
        print("Now sleep for 10 seconds...")
        time.sleep(10)
        start_server(server_id)
    else:
    	for i in range(2, arg_len):
		server_id = int(sys.argv[i])
		if server_id < num_of_servers:
			kill_server(server_id)
        # sleep for 10 seconds
        print("Now sleep for 10 seconds...")
        time.sleep(10)    
	for i in range(2, arg_len):
 		server_id = int(sys.argv[i])
		if server_id < num_of_servers:
			start_server(server_id, filename = config_file)   
if __name__ == "__main__":
    reboot()
