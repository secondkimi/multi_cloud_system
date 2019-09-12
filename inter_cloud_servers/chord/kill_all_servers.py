import os
import socket
import sys

def  kill_servers():
    if len(sys.argv) < 2:
	config_file = "serverConfig.conf"
    else:
    	config_file = sys.argv[1]

    f = open(config_file)
    num_of_servers = int(f.readline()) + 1 # including the stat manager
    f.close()
    temp_file = "temp"
    cmd_str = "ps -ef | grep build/libs/ChordServer-1.0-SNAPSHOT-all.jar > {}".format(temp_file)
    os.system(cmd_str)
    # Also kill the stat manager
    cmd_str = "ps -ef | grep build/libs/StatMgr-1.0-SNAPSHOT.jar >> {}".format(temp_file)
    os.system(cmd_str)

    f = open(temp_file)
    line = 1
    while num_of_servers > 0:
        line = f.readline()
        if not line:
            break  
        splits = line.split()
        if len(splits) < 2:
            break
        to_kill = splits[1]
        cmd_str = 'kill -9 {}'.format(to_kill)
        ret = os.system(cmd_str)
        num_of_servers -= 1
        if ret != 0:
            print('error in killing {}'.format(to_kill))

    f.close()
    cmd_str = "rm {}".format(temp_file)
    os.system(cmd_str) 


if __name__ == "__main__":
    kill_servers()
