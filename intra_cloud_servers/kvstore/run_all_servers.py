import os
import socket
import sys

def run_server():
    if len(sys.argv) < 2:
    	config_file = "serverConfig.txt"
    else:
        config_file = sys.argv[1]
    f = open(config_file)
    num_of_servers = int(f.readline())
    print(num_of_servers)
    hostname = socket.gethostname()
    print(hostname)
    IPAddr = socket.gethostbyname(hostname)
    print(IPAddr)
    cmd_str = "rm *.ser"
    os.system(cmd_str)
    line = num_of_servers
    servers = []
    count = 0
    while line and num_of_servers > count:
        line = f.readline()
        servers.append(line)
        count += 1

    for i in range(num_of_servers):
        line = servers[i]
        splits = line.split(":")
        print(splits,IPAddr)
        if splits[0] != IPAddr:
            continue

        cmd_str = 'java -ea -jar build/libs/kvServer-1.0-SNAPSHOT-all.jar {} {} -l &'.format(i, config_file)
        ret = os.system(cmd_str)
        print(ret)
    f.close()
     


if __name__ == "__main__":
    run_server()
