import os
import sys
import time

def kill_server(server_id):
    temp_file = "temp"
    cmd_str = "ps -ef | grep \'build/libs/kvServer-1.0-SNAPSHOT-all.jar {}\' > {}".format(server_id, temp_file)
    os.system(cmd_str)

    f = open(temp_file)
    line = f.readline()
    if not line:
	return
 
    splits = line.split()
    if len(splits) < 2:
        return
    to_kill = splits[1]
    print(to_kill)
    cmd_str = 'kill -9 {}'.format(to_kill)
    ret = os.system(cmd_str)
    print(ret)
    if ret != 0:
        print('error in killing {}'.format(to_kill))
        return

    f.close()
    print("server {} has been killed".format(server_id))
    cmd_str = "rm {}".format(temp_file)
    os.system(cmd_str) 


def kill_cmd():
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
    else:
    	for i in range(2, arg_len):
		server_id = int(sys.argv[i])
		if server_id < num_of_servers:
			kill_server(server_id)
    
if __name__ == "__main__":
    kill_cmd()
