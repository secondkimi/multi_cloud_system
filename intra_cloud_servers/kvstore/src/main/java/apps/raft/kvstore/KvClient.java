package apps.raft.kvstore;

import kvstore.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.sql.Timestamp;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Time;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KvClient {
    private final ManagedChannel[] channels;
    private final KeyValueStoreGrpc.KeyValueStoreBlockingStub[] stubs;
    public static String configFileName = "serverConfig.txt";
    private String[] ips;
    private String[] ports;
    private ReentrantLock mu;
    private int completedPutCount = 0;
    private int completedGetCount = 0;
    ExecutorService executor;
    private Date date;
    private long totalClientLatency;
    private long numOfClientReq;
    public static int passCount;
    public static int failCount;
    private int numOfServers;


    public KvClient(String[] ips, String[] ports) {
        date = new Date();
        executor = Executors.newFixedThreadPool(30);
        mu = new ReentrantLock();
        System.out.println(ips.toString());
        numOfServers = ips.length;
        this.channels = new ManagedChannel[numOfServers];
        this.stubs = new KeyValueStoreGrpc.KeyValueStoreBlockingStub[numOfServers];
        this.ips = ips;
        this.ports = ports;
        System.out.println("start connecting...");
        for (int i = 0; i < ips.length; i++) {
            System.out.println("connecting " + ips[i] + ":" + ports[i]);
            this.channels[i] = ManagedChannelBuilder.forAddress(ips[i], Integer.parseInt(ports[i])).usePlaintext(true).build();
            this.stubs[i] = KeyValueStoreGrpc.newBlockingStub(this.channels[i]);
        }
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("> ");
        while (true) {
            String cmd = scanner.nextLine();
            String[] splitCmd = cmd.split(" ");
            if (!valid(splitCmd)) {
                System.out.println("INFO:only put/get/getState command is supported");
                System.out.print("> ");
                continue;
            }
            if (splitCmd[0].equalsIgnoreCase("put")) {
                Thread putThread = new Thread(){
                    @Override
                    public void run(){
                        boolean result = putValue(splitCmd[1], splitCmd[2], Integer.parseInt(splitCmd[3]), 0, false);
                        if(!result) {
                            System.out.println("put request for "+splitCmd[1]+" failed after three retries");
                        }
                        System.out.print("> ");
                    }

                };
                putThread.start();
            } else if (splitCmd[0].equalsIgnoreCase("get")) {
                try {
                    Thread getThread = new Thread(){
                        @Override
                        public void run(){
                            try {
                                String val = getValue(splitCmd[1], Integer.parseInt(splitCmd[2]), 0);
                                if(val == null) {
                                    System.out.println("the key "+splitCmd[1]+" does not exist");
                                }
                                System.out.print("> ");
                            } catch (Exception e) {
                                System.out.println("INFO:failed to get the key-value");
                                System.out.print("> ");
                            }
                        }

                    };
                    getThread.start();
                } catch (Exception e) {
                    System.out.println("INFO:failed to get the key-value");
                }
            } else if(splitCmd[0].equalsIgnoreCase("getState")){
                getState(Integer.parseInt(splitCmd[1]));
                System.out.print("> ");
            }
        }
    }

    public boolean valid(String[] splitCmd) {
        if (!splitCmd[0].equalsIgnoreCase("put") && !splitCmd[0].equalsIgnoreCase("get") && !splitCmd[0].equalsIgnoreCase("getState")) {
            return false;
        }

        int index = 0;
        String cmd = splitCmd[0].toLowerCase();

        switch (cmd) {
            case "put":
                if (splitCmd.length != 4) {
                    System.out.println("INFO:put command should in the format: put [key] [value] [server_index]");
                    return false;
                }

                try {
                    int serverIndex = Integer.parseInt(splitCmd[3]);
                } catch (NumberFormatException e) {
                    System.out.println("INFO:put command should in the format: put [key] [value] [server_index]");
                    return false;
                }
                index = Integer.parseInt(splitCmd[3]);
                if (!(index >= 0 && index < channels.length)) {
                    System.out.println("INFO:index must be within range from 0 to " + (channels.length-1));
                    return false;
                }
                return true;
            case "get":
                if (splitCmd.length != 3) {
                    System.out.println("INFO:get command should in the format: get [key] [server_index]");
                    return false;
                }

                try {
                    int serverIndex = Integer.parseInt(splitCmd[2]);
                } catch (NumberFormatException e) {
                    System.out.println("INFO:get command should in the format: get [key] [server_index]");
                    return false;
                }
                index = Integer.parseInt(splitCmd[2]);
                if (!(index >= 0 && index < channels.length)) {
                    System.out.println("INFO:index must be within range from 0 to " + (channels.length-1));
                    return false;
                }
                return true;

            case "getstate":
                if (splitCmd.length != 2) {
                    System.out.println("INFO:getState command should in the format: getState [index]");
                    return false;
                }

                return true;
            default:
                return false;

        }

    }

    public void getState(int server) {
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = KeyValueStoreGrpc.newBlockingStub(this.channels[server]);
        }

        System.out.println("------------------------------------------------------------");
        Kvstore.StateResponse response =  this.stubs[server].getServerState(Kvstore.EmptyRequest.newBuilder().build());
        System.out.println("server " + server + " state is as follows: ");
        System.out.println("term: " + response.getTerm());
        System.out.println("votedFor: " + response.getVoteFor());
        System.out.println("logLength: " + response.getLogLength());
        if (response.getRole() == 0) {
            System.out.println("role: LEADER");
        } else if (response.getRole() == 1) {
            System.out.println("role: CANDIDATE");
        } else {
            System.out.println("role: FOLLOWER");
        }

        System.out.println("commitIndex: " + response.getCommitIndex());
        System.out.println("lastApplied: " + response.getLastApplied());
        System.out.println("leaderId: " + response.getLeaderId());
        System.out.println("------------------------------------------------------------");
    }

    public boolean putValue(String key, String value, int server, int times, boolean testLatency) {
        Kvstore.PutRequest.Builder builder = Kvstore.PutRequest.newBuilder();
        builder.setKey(key);
        builder.setValue(value);
        builder.setServerIndex(-1);
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = KeyValueStoreGrpc.newBlockingStub(this.channels[server]);
        }
        long startTime = 0;
        long endTime = 0;
        Kvstore.PutResponse response = null;
        try {
            startTime = System.nanoTime();
            response = this.stubs[server].withDeadlineAfter(5000, TimeUnit.MILLISECONDS).put(builder.build());
        } catch (Exception e) {
            System.out.println("Exception in sending put request to server : " + String.valueOf(server) + e.toString());
        }

        if (response != null && response.getRet().equals(Kvstore.ReturnCode.SUCCESS)) {
            // System.out.println("put done");
            long timeElapsed = 0;
            if (testLatency) {
                endTime = System.nanoTime();
                timeElapsed = (endTime - startTime) / 1000000;
                //System.out.println("client request takes "+timeElapsed+" MILLISECONDS");
            }
            
            try {
                mu.lock();
                numOfClientReq++;
                totalClientLatency += timeElapsed;
                completedPutCount += 1;
            } finally {
                mu.unlock();
            }
            return true;
        } else {
            if (times >= 2) {
                // System.out.println("failed 3 times, stop retry.");
                return false;
            }
            // System.out.println("retrying on server " + server);
            return putValue(key, value, server, times+1, testLatency);
        }
    }

    public String getValue(String key, int server, int times) throws Exception {
        Kvstore.GetRequest.Builder builder = Kvstore.GetRequest.newBuilder();
        builder.setKey(key);
        Kvstore.GetResponse response = null;
        long startTime = 0;
        long endTime = 0;
        try {
            if (channels[server].isTerminated() || channels[server].isShutdown()) {
                this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
                this.stubs[server] = KeyValueStoreGrpc.newBlockingStub(this.channels[server]);
            }
            startTime = System.nanoTime();
            response = this.stubs[server].withDeadlineAfter(5000, TimeUnit.MILLISECONDS).get(builder.build());
        } catch (Exception e) {
            System.out.println("Exception in sending get request to server : " + String.valueOf(server) + e.toString());
        }

        if (response == null || !response.getRet().equals(Kvstore.ReturnCode.SUCCESS)) {
            if (times >= 2) {
                System.out.println("failed 2 times, stop retry.");
                return "";
            }
            System.out.println("retrying on server " + server);
            return getValue(key, server, times+1);
        } else {
            long timeElapsed = 0;
            endTime = System.nanoTime();
            timeElapsed = (endTime - startTime) / 1000000;
            try {
                mu.lock();
                numOfClientReq++;
                totalClientLatency += timeElapsed;
                completedGetCount += 1;
            } finally {
                mu.unlock();
            }

            //System.out.println("The value is: " + response.getValue());
        }
        if (response != null) {
            return response.getValue();
        } else {
            return null;
        }
    }

    public boolean simplePutGetTest() {
        int numOfServers = channels.length;
        String key = "name";
        boolean putRes = putValue(key, "zhaowei", 0, 0, false);
        try {
            Thread.sleep(200*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }
        for (int i = 0; i < numOfServers; i++) {
            String value;
            try {
                value = getValue(key, i, 0);
                assert  (value.equals("zhaowei"));
            } catch (Exception e) {
                System.out.println("failed to get value");
                return false;
            }
        }
        return true;
    }

    public void testReconnect() {

        // Test cutting off one server.  keep requesting server to leader Info, 
        // as long as we see consensus leader, assume the server converges

        // Then test partition first. keep requesting server to leader Info, as long as
        // we see consensus leader, assume the server converges

    }

    public void testSimplePartition() {
        int n = ips.length;
        int n1 = (n+1) / 2;
        int n2 = n/2;
        ChaosClient chaosClient = new ChaosClient(ips, ports);
        List<List<Float>> matrix = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            matrix.add(new ArrayList<Float>());
            List<Float> row = matrix.get(i);
            for (int j = 0; j < n; j++) {
                row.add(0.0F);
            }
        }

        for (int left = 0; left < n1; left++) {
            for (int right = n1; right < n; right++) {
                List<Float> row1 = matrix.get(left);
                List<Float> row2 = matrix.get(right);
                row1.set(right, 1.0F);
                row2.set(left, 1.0F);
            }
        }

        System.out.println("partition matrix");
        for (int i = 0; i < n; i++) {
            List<Float> row = matrix.get(i);
            System.out.println(Arrays.toString(row.toArray()));
        }

        /*
         * left part:  0, 1, ..., n1-1
         * right part: n1, n1+1, ..., n-2
         */
        // no leader in right part, one leader in left part.
        System.out.println("uploading chaosmonkey matrix");
        for (int i = 0; i < n; i++) {
            boolean res = chaosClient.uploadMat(matrix, i);
            assert (res);
        }

        System.out.println("upload chaosmonkey matrix done");
        /*
        try {
            Thread.sleep(400*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }


        // eliminate partition
        for (int i = 0; i < n; i++) {
            List<Float> row = matrix.get(i);
            for (int j = 0; j < n; j++) {
                row.set(j, 0.0F);
            }
        }

        for (int i = 0; i < n; i++) {
            chaosClient.uploadMat(matrix, i);
        }

        try {
            Thread.sleep(800*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }*/

    }

    public void testPartition() {
        // partition test with chaosmonkey
        int n = ips.length;
        // partition into two parts, n/2 and (n+1)/2
        int n1 = (n+1)/2;
        int n2 = n/2;

        String key = "name";
        String val1 = "zhaowei";
        String val2 = "tianyi";

        boolean result = putValue(key, val1, 0, 0, false);
        assert (result);
        try {
            Thread.sleep(200*numOfServers);
        } catch (Exception e) {
            System.out.println(e);

        }
        for (int i = 0; i < n; i++) {
            try {
                String testVal = getValue(key, 0, 0);
                assert (testVal.equals(val1));
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        System.out.println("PASS0: get the expected value from each server");
        passCount++;

        ChaosClient chaosClient = new ChaosClient(ips, ports);


        int leaderCount = 0;
        int prevLeaderId = -1;
        for (int i = 0; i < n; i++) {
            if (channels[i].isTerminated() || channels[i].isShutdown()) {
                this.channels[i] = ManagedChannelBuilder.forAddress(ips[i], Integer.parseInt(ports[i])).usePlaintext(true).build();
                this.stubs[i] = KeyValueStoreGrpc.newBlockingStub(this.channels[i]);
            }
            Kvstore.StateResponse response = this.stubs[i].getServerState(Kvstore.EmptyRequest.newBuilder().build());
            if (response.getRole() == 0) {
                prevLeaderId = i;
                leaderCount += 1;
            }
        }

        assert (leaderCount == 1);
        System.out.println("PASS1: before partition, one leader in the cluster, the leader id is " + prevLeaderId);
        passCount++;

        List<List<Float>> matrix = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            matrix.add(new ArrayList<Float>());
            List<Float> row = matrix.get(i);
            for (int j = 0; j < n; j++) {
                row.add(0.0F);
            }
        }

        for (int left = 0; left < n1; left++) {
            for (int right = n1; right < n; right++) {
                List<Float> row1 = matrix.get(left);
                List<Float> row2 = matrix.get(right);
                row1.set(right, 1.0F);
                row2.set(left, 1.0F);
            }
        }

        System.out.println("partition matrix");
        for (int i = 0; i < n; i++) {
            List<Float> row = matrix.get(i);
            System.out.println(Arrays.toString(row.toArray()));
        }


        /*
         * left part:  0, 1, ..., n1-1
         * right part: n1, n1+1, ..., n-2
         */
        // no leader in right part, one leader in left part.
        System.out.println("uploading chaosmonkey matrix");
        for (int i = 0; i < n; i++) {
            boolean res = chaosClient.uploadMat(matrix, i);
            assert (res);
        }

        System.out.println("upload chaosmonkey matrix done");
        try {
            Thread.sleep(400*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }

        // leftpart
        int curLeaderId = -1;
        leaderCount = 0;
        for (int left = 0; left < n1; left++) {
            if (channels[left].isTerminated() || channels[left].isShutdown()) {
                this.channels[left] = ManagedChannelBuilder.forAddress(ips[left], Integer.parseInt(ports[left])).usePlaintext(true).build();
                this.stubs[left] = KeyValueStoreGrpc.newBlockingStub(this.channels[left]);
            }
            Kvstore.StateResponse response = this.stubs[left].getServerState(Kvstore.EmptyRequest.newBuilder().build());
            if (response.getRole() == 0) {
                curLeaderId = left;
                leaderCount += 1;
            }
        }

        assert (leaderCount == 1);
        if (prevLeaderId < n1) {
            assert (curLeaderId == prevLeaderId);
        } else {
            assert (curLeaderId != prevLeaderId);
        }
        System.out.println("PASS2: left part has one leader");
        passCount++;

        // right part
        leaderCount = 0;
        curLeaderId = -1;
        for (int right = n1; right < n; right++) {
            if (channels[right].isTerminated() || channels[right].isShutdown()) {
                this.channels[right] = ManagedChannelBuilder.forAddress(ips[right], Integer.parseInt(ports[right])).usePlaintext(true).build();
                this.stubs[right] = KeyValueStoreGrpc.newBlockingStub(this.channels[right]);
            }
            Kvstore.StateResponse response = this.stubs[right].getServerState(Kvstore.EmptyRequest.newBuilder().build());
            if (response.getRole() == 0) {
                leaderCount += 1;
                curLeaderId = right;
            }
        }

        if (prevLeaderId >= n1) {
            assert (leaderCount == 1 && curLeaderId == prevLeaderId);
            System.out.println("PASS2: right part has one old leader");
        } else {
            assert (leaderCount == 0);
            System.out.println("PASS2: right part has no leader");
        }
        passCount++;

        // call put in left part, should succeed.
        boolean putRes = putValue(key, val2, 0, 0, false);
        assert (putRes);
        try {
            Thread.sleep(400*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("put succeed in server 0, majority: " + val2);
        String testval2 = null;
        for (int i = 0; i < n1; i++) {
            try {
                String testVal = getValue(key, i, 0);
                testval2 = testVal;
                assert (testVal.equals(val2));
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        System.out.println("PASS3: put/get the correct value in majority servers, value is " + testval2);
        passCount++;
        try {
            Thread.sleep(200*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }
        for (int i = n1; i < n; i++) {
            putRes = putValue(key, val2, i, 0, false);
            assert (!putRes);
        }

        for (int i = n1; i < n; i++) {
            try {
                String testVal = getValue(key, i, 0);
                testval2 = testVal;
                assert (testVal.equals(val1));
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        System.out.println("PASS4: put failed in minority cluster. Value is old value " + testval2);
        passCount++;

        // eliminate partition
        for (int i = 0; i < n; i++) {
            List<Float> row = matrix.get(i);
            for (int j = 0; j < n; j++) {
                row.set(j, 0.0F);
            }
        }

        for (int i = 0; i < n; i++) {
            chaosClient.uploadMat(matrix, i);
        }

        try {
            Thread.sleep(800*numOfServers);
        } catch (Exception e) {
            System.out.println(e);
        }

        for (int i = n1; i < n; i++) {
            try {
                String testVal = getValue(key, i, 0);
                testval2 = testVal;
                assert (testVal.equals(val2 ));
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        System.out.println("PASS5: After eliminate partition, minority servers get the newest put value " + testval2);
        passCount++;
        prevLeaderId = curLeaderId;
        curLeaderId = -1;

        leaderCount = 0;
        for (int i = 0; i < n; i++) {
            if (channels[i].isTerminated() || channels[i].isShutdown()) {
                this.channels[i] = ManagedChannelBuilder.forAddress(ips[i], Integer.parseInt(ports[i])).usePlaintext(true).build();
                this.stubs[i] = KeyValueStoreGrpc.newBlockingStub(this.channels[i]);
            }
            Kvstore.StateResponse response = this.stubs[i].getServerState(Kvstore.EmptyRequest.newBuilder().build());
            if (response.getRole() == 0) {
                curLeaderId = i;
                leaderCount += 1;
            }
        }

        assert(curLeaderId >= 0 && curLeaderId < n1);
        assert(leaderCount == 1);
        System.out.println("PASS6: After eliminate partition, the cluster has one leader in left part, leader id is: " + curLeaderId);
        passCount++;

    }

    public String makeString(int sizeInBytes, int step) {
        int len = sizeInBytes / 2;
        StringBuilder builder = new StringBuilder();
        String charString = "abcdefghijklmnopqrstuvwxyz";
        char[] charArray = charString.toCharArray();
        for (int i = 0; i < len; i++) {

            builder.append(charArray[(i*step)%charArray.length]);
        }
        return builder.toString();
    }

    public void testConcurrent() {
        completedPutCount = 0;
        numOfClientReq = 0;
        totalClientLatency = 0;

        System.out.println("Start concurrent & throughput test");
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter the key size (in bytes): ");
        int keySize = scanner.nextInt();
        String key = makeString(keySize, 1);
        String value = makeString(keySize, 2);

        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Callable<Void> task = () -> {
                for (int j = 0; j < 1000; j++) {
                    putValue(key, value, 0, 0, true);
                }
                return null;
            };
            futures.add(executor.submit(task));
        }
        int test_span = 15; // test average completed client requests for 15 seconds
        int tick = 0;
        try{
            Thread.sleep(200*numOfServers);
        } catch (Exception e) {

        }
        int initial = completedPutCount;
        while (test_span > 0) {
            try {
                int completed = (test_span == 15) ? 0 : completedPutCount - initial;
                System.out.println("After "+tick+" seconds...");
                System.out.println(new Timestamp(new Date().getTime()) + ": Number of completed put request: " + completed);
                if(completedPutCount == 10 * 1000) break;
                test_span--;
                tick++;
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        double avgPut = (completedPutCount-initial) / (15.0 - test_span);
        if(numOfClientReq > 0) {
            long avgClientLatency = totalClientLatency / numOfClientReq;
            System.out.println("Average client put latency is "+avgClientLatency+" MILLISECONDS");
        }
        
        System.out.println("average client request processed per second: "+ avgPut);

    }

    public void testConcurrentGet() {
        System.out.println("Start concurrent & throughput test");
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter the key size (in bytes): ");
        int keySize = scanner.nextInt();
        String key = makeString(keySize, 1);
        String value = makeString(keySize, 2);
        putValue(key, value, 0, 0, false);
        completedGetCount = 0;
        numOfClientReq = 0;
        totalClientLatency = 0;
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int index = i % ports.length;
            Callable<Void> task = () -> {
                for (int j = 0; j < 1000; j++) {
                    getValue(key, index, 0);
                }
                return null;
            };
            futures.add(executor.submit(task));
        }
        int test_span = 15; // test average completed client requests for 15 seconds
        try{
            Thread.sleep(10*numOfServers);
        } catch (Exception e) {

        }
        
        int initial = completedGetCount;
        while (test_span > 0) {
            try {
                
                int completed = (test_span == 15) ? 0 : completedGetCount - initial;
                System.out.println("After "+(15-test_span)+" seconds...");
                System.out.println(new Timestamp(new Date().getTime()) + ": Number of completed get request: " + completed);
                if (completedGetCount == 10 * 1000)
                    break;
                test_span--;
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        double avgPut = (completedGetCount-initial) / (15.0 - test_span);
        if(numOfClientReq > 0) {
            long avgClientLatency = totalClientLatency / numOfClientReq;
            System.out.println("Average client put latency is "+avgClientLatency+" MILLISECONDS");
        }

        System.out.println("average client request processed per second: "+ avgPut);
    }

    public void testReport() {
        if(failCount == 0) {
            System.out.println("All "+passCount+" Tests Passed");
        } else{
            System.out.println("Passed Tests: "+passCount);
            System.out.println("Failed Tests: "+failCount);
        }
    }


    public static void main(String args[]) throws Exception {
        // todo: read server addresses and create the KvClient. Should support command line operation ?
        if (args.length == 2) {
            KvClient.configFileName = args[1];
        }
        File file = new File(KvClient.configFileName);
        BufferedReader br = new BufferedReader(new FileReader(file));
        int numOfServers = Integer.parseInt(br.readLine());
        String st;
        String[] ips = new String[numOfServers];
        String[] ports = new String[numOfServers];
        System.out.println("start reading files");
        for (int i = 0; i < numOfServers; i++) {
            st = br.readLine();
            String[] sp = st.split(":");
            ips[i] = sp[0];
            ports[i] = sp[1];
        }
        System.out.println("after read config");
        KvClient client = new KvClient(ips, ports);
        if (args.length != 0 && args[0].equals("-t")) {
            if (client.simplePutGetTest()) {
                System.out.println("INFO: simple test passed");
            } else {
                System.out.println("INFO: simple test failed");
            }
            System.out.println("*****************************************************************************");
            System.out.println("Start testing partition with chaosmonkey.........");
            client.testPartition();
            System.out.println("*****************************************************************************");
            System.out.println("Start testing concurrent put.........");
            client.testConcurrent();
            System.out.println("*****************************************************************************");
            System.out.println("Start testing concurrent get.........");
            client.testConcurrentGet();
            System.out.println("*****************************************************************************");
            client.testReport();
        } else if(args.length != 0 && args[0].equals("-cp")) {
            // test concurrent client throughput and latency only
            System.out.println("*****************************************************************************");
            System.out.println("Start testing concurrent put.........");
            client.testConcurrent();
        } else if(args.length != 0 && args[0].equals("-cg")) {
            System.out.println("*****************************************************************************");
            System.out.println("Start testing concurrent get.........");
            client.testConcurrentGet();
            System.out.println("*****************************************************************************");
        }
        else if(args.length != 0 && args[0].equals("-r")) {
            // reconnect test.
            client.testReconnect();
        }  else if (args.length != 0 && args[0].equals("-pt")) {
            client.testSimplePartition();
        }
        else {
            client.start();
        }
        System.exit(0);
    }


}
