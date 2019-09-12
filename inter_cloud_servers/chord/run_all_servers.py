import os
import socket
import sys
import time

def run_servers_all_alive():

    config_file = "serverConfig.conf"
    if len(sys.argv) >= 2:
        config_file = sys.argv[1]
    f = open(config_file)
    num_of_servers = int(f.readline())
    print(num_of_servers)
    hostname = socket.gethostname()
    print(hostname)
    IPAddr = socket.gethostbyname(hostname)
    print(IPAddr)
    line = num_of_servers
    servers = []
    count = 0
    while line and num_of_servers > count:
        line = f.readline()
        servers.append(line)
        count += 1

    # first start stat manager
    print("Starting stat manager")
    cmd_str = 'java -ea -jar build/libs/StatMgr-1.0-SNAPSHOT.jar &'
    ret = os.system(cmd_str)
    print(ret)
    for i in range(num_of_servers):
        line = servers[i]
        splits = line.split(":")
        print(splits,IPAddr)
        #if splits[0] != IPAddr:
        #    continue
        if i == 0:
            # the first server, start it.
            cmd_str = 'java -ea -jar build/libs/ChordServer-1.0-SNAPSHOT-all.jar {} -i {} &'.format(config_file, i)
            ret = os.system(cmd_str)
            print(ret)
        elif i < 5: # i = 1, 2, 3, 4
            time.sleep(3)
            cmd_str = 'java -ea -jar build/libs/ChordServer-1.0-SNAPSHOT-all.jar {} -i {} -j 0 &'.format(config_file, i)
            ret = os.system(cmd_str)
            print(ret)
        else:
            # join first server.
            cmd_str = 'java -ea -jar build/libs/ChordServer-1.0-SNAPSHOT-all.jar {} -i {} -j {} &'.format(config_file, i, i%4)
            ret = os.system(cmd_str)
            print(ret)

    f.close()


if __name__ == "__main__":

    run_servers_all_alive()
