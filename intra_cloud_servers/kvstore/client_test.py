import os
import sys

def client_test():
	if len(sys.argv) == 1:
		# no serverConfig specified, using the default.
		cmd = "java -ea -jar build/libs/kvClient-1.0-SNAPSHOT.jar -t"
	elif len(sys.argv) == 2:
		cmd = "java -ea -jar build/libs/kvClient-1.0-SNAPSHOT.jar -t {}".format(sys.argv[1])
	else:
		cmd = "java -ea -jar build/libs/kvClient-1.0-SNAPSHOT.jar {} {}".format(sys.argv[2], sys.argv[1])
	os.system(cmd)

if __name__ == "__main__":
	client_test()
