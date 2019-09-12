package apps.raft.kvstore;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.Thread;

import chaosmonkey.*;
import kvstore.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.FileNotFoundException; 
import java.io.FileOutputStream; 
import java.io.PrintStream; 

import java.util.Date;
import java.sql.Timestamp;

/*
 * Author: Zhaowei Huang and Tianyi Wang
 */
public class KvServer{

    public enum RaftRole {
        LEADER(0),
        CANDIDATE(1),
        FOLLOWER(2);

        private final int value;

        private RaftRole(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
   /*
    * KvStore variables
    */
    private float[][] chaosMatrix;
    private HashMap<String, String> kvMap;
    private final ManagedChannel[] channels;
    private final KeyValueStoreGrpc.KeyValueStoreBlockingStub[] stubs;
    private  Server server;
    private int numOfServers;
    private int port;
    private int serverIndex;
    private String[] ips;
    private String[] ports;

    /*
     * Raft server state variables
     */

    private int[] inFlightRequest;
    RaftRole role = RaftRole.FOLLOWER;
    int electionTimeOutInMs;
    long timeout;
    int leaderId = -1; // follower redirect clients

    int currentTerm;
    int votedFor;
    //List<Kvstore.Log> logs;
    CopyOnWriteArrayList<Kvstore.Log> logs;

    // candidate paramters
    int numOfVotesReceived;

    int commitIndex;
    int lastApplied;
    int[] nextIndex;
    int[] matchIndex;

    boolean hasPersistentChanged;
    String serFilename;


    private static final int MAX_RPC_THREADS = 10;
    private static final int STARTUP_OVERHEAD_TIME = 6000;

    /* leader parameters */
    private static int HEARTBEAT_INTERVAL = 10;
    private static int MIN_ELECTION_TIMEOUT = 150;
    private static int RPC_TIMEOUT = 1000;
    private static int MAX_NUM_THREADS = 60;

    private PrintStream outStream;
    private PrintStream errStream;
    private Date date;
    /*
     * lock & thread pool
     */
    ReentrantLock mu = new ReentrantLock();
    ExecutorService executor;

    public KvServer(String[] ips, String[] ports, int index, boolean printLogger) {
        
        date = new Date();
        if(printLogger) {
            // print std and stderr to logger file
            try {
                FileOutputStream stdOut = new FileOutputStream("server"+index+".log");
                FileOutputStream stdErr = new FileOutputStream("server"+index+".err");
                outStream = new PrintStream(stdOut);
                errStream = new PrintStream(stdErr);
                System.setOut(outStream);
                System.setErr(errStream);
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
            }
        }
        executor= Executors.newFixedThreadPool(MAX_NUM_THREADS);
        this.port = Integer.parseInt(ports[index]);
        this.serverIndex = index;
        this.numOfServers = ips.length;
        this.channels = new ManagedChannel[this.numOfServers];
        this.stubs = new KeyValueStoreGrpc.KeyValueStoreBlockingStub[this.numOfServers];
        this.chaosMatrix = new float[numOfServers][numOfServers];
        this.ips = ips;
        this.ports = ports;
        this.inFlightRequest = new int[ips.length];
        kvMap = new HashMap<>();
        nextIndex = new int[numOfServers];
        matchIndex = new int[numOfServers];
        lastApplied = -1;
        commitIndex = -1;
        serFilename = "server_state_"+this.serverIndex+".ser"; 
        // read from serializable
        File inputFile = new File(serFilename);
        // read volatile states from the disk if reboot.
        if(inputFile.exists()) {
            try {
                System.out.println(new Timestamp((new Date()).getTime())+": "+".ser file exist");
                FileInputStream fileIn = new FileInputStream(serFilename);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                PersistentState state = (PersistentState) in.readObject();
                in.close();
                fileIn.close();
                if (state != null) {
                    //logs = state.getLogs();
                    logs = new CopyOnWriteArrayList(state.getLogs());
                    currentTerm = state.getCurrentTerm();
                    votedFor = state.getVotedFor();
                } else {
                    //logs = new ArrayList<>();
                    logs = new CopyOnWriteArrayList<Kvstore.Log>();
                    currentTerm = 0;
                    votedFor =  -1;
                }
                
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(new Timestamp((new Date()).getTime())+": "+"error in read the file");
            }
        } else {
            //logs = new ArrayList<>();
            logs = new CopyOnWriteArrayList<Kvstore.Log>();
            currentTerm = 0;
            votedFor =  -1;
        }
        
        for(int i = 0; i < numOfServers; i++) {
            matchIndex[i] = -1;
            nextIndex[i] = 0;
            if (i != serverIndex) {
                channels[i] = ManagedChannelBuilder.forAddress(ips[i], Integer.parseInt(ports[i])).usePlaintext(true).build();
                stubs[i] = KeyValueStoreGrpc.newBlockingStub(channels[i]);
            }
        }
        resetTimeout(true);


    }

    public void start(int numThreads) throws IOException {
        server = ServerBuilder.forPort(this.port)
                .addService(new KeyValueStoreImpl())
                .addService(new ChaosMonkeyImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();

        System.out.println(new Timestamp((new Date()).getTime())+": server starts at port: " + this.port);
        Thread monitorThread = new Thread(){
                @Override
                public void run(){
                    serverRun();
               }

        };
        monitorThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                KvServer.this.stop();
            }
        });
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void resetTimeout(boolean isElectionTimeOut) {
        if(isElectionTimeOut) {
            electionTimeOutInMs = new Random().nextInt(MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT;
        }
        timeout = System.currentTimeMillis() + electionTimeOutInMs;

    }

    public void tryApply() {

        while (true) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
            }
            // write volative states to the disk
            if(hasPersistentChanged){
                PersistentState currState = new PersistentState(currentTerm, votedFor, logs, mu);
                FileOutputStream fos = null;
                ObjectOutputStream out = null;
                try {
                    fos = new FileOutputStream(serFilename);
                    out = new ObjectOutputStream(fos);
                    out.writeObject(currState);
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println(new Timestamp((new Date()).getTime())+": "+"exception in write obj");
                }

                hasPersistentChanged = false;
            }
            if (commitIndex <= lastApplied) {
                continue;
            }
            try {
                mu.lock();
                while (commitIndex > lastApplied) {
                    lastApplied += 1;
                    System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" apply index " + lastApplied);
                    Kvstore.Log entry = logs.get(lastApplied);
                    String key = entry.getPutReq().getKey();
                    String value = entry.getPutReq().getValue();
                    kvMap.put(key, value);
                }
                
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
            } finally {
                mu.unlock();
            }

        }
    }

    public void sendAppendEntry(int index) {
        Kvstore.AppendResponse response = null;
        Kvstore.AppendRequest appendRequest = null;
        try {
            mu.lock();
            Kvstore.AppendRequest.Builder appendBuilder = Kvstore.AppendRequest.newBuilder();
            appendBuilder.setTerm(currentTerm);
            appendBuilder.setLeaderId(serverIndex);
            int toSendEntryIndex = nextIndex[index];


            appendBuilder.setPrevLogIndex(toSendEntryIndex - 1);
            if (toSendEntryIndex <= 0) {
                appendBuilder.setPrevLogTerm(currentTerm); // empty log list
            } else {
                appendBuilder.setPrevLogTerm(logs.get(toSendEntryIndex - 1).getTerm());
            }

            Kvstore.Log entry = null;
            if (toSendEntryIndex < logs.size() && logs.size() > 0) {
                entry = logs.get(toSendEntryIndex);
                appendBuilder.setIsHeartbeat(false);
            } else {
                Kvstore.Log.Builder logBuilder = Kvstore.Log.newBuilder();
                logBuilder.setTerm(currentTerm);
                entry = logBuilder.build();
                appendBuilder.setIsHeartbeat(true);
            }
            appendBuilder.setEntries(entry);
            appendBuilder.setLeaderCommit(commitIndex);
            if (channels[index].isShutdown() || channels[index].isTerminated()) {
                channels[index] = ManagedChannelBuilder.forAddress(ips[index], Integer.parseInt(ports[index])).usePlaintext(true).build();
                stubs[index] = KeyValueStoreGrpc.newBlockingStub(channels[index]);
            }
            appendRequest = appendBuilder.build();
            response = null;
            inFlightRequest[index] += 1;
        } finally {
            mu.unlock();
        }

        try {
            response = stubs[index].withDeadlineAfter(500, TimeUnit.MILLISECONDS).appendEntries(appendRequest);
        } catch (Exception e) {
            System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
        }

        try {
            mu.lock();
            inFlightRequest[index] -= 1;

            if (response == null) {
                return;
            }

            if (response.getSuccess()) {
                if (!appendRequest.getIsHeartbeat()) {
                    nextIndex[index] += 1;
                    nextIndex[index] = Math.min(nextIndex[index], logs.size());
                    matchIndex[index] = nextIndex[index] - 1;
                }
            } else {
                if (response.getTerm() > currentTerm) {
                    becomeFollower(response.getTerm(), index);
                } else {
                    nextIndex[index] -= 1;
                    nextIndex[index] = Math.max(nextIndex[index], 0);
                }

            }
        } catch (Exception e) {
            System.err.println(new Timestamp((new Date()).getTime()) + ": server " + serverIndex + " Exception in sendAppendEntry to " + index + ": " + e.toString());
        } finally {
            mu.unlock();
        }
    }

    public void sendRequestVote(int index) {
        try {
            mu.lock();
            Kvstore.VoteRequest.Builder voteBuilder = Kvstore.VoteRequest.newBuilder();

            voteBuilder.setTerm(currentTerm);
            voteBuilder.setCandidateId(serverIndex);
            int lastLogIdx = logs.size() - 1;
            voteBuilder.setLastLogIndex(lastLogIdx);
            if(lastLogIdx >= 0) {

                voteBuilder.setLastLogTerm(logs.get(lastLogIdx).getTerm());
            } else {
                voteBuilder.setLastLogTerm(currentTerm);
            }

            System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" sending request vote to "+index);
            if (channels[index].isShutdown() || channels[index].isTerminated()) {
                channels[index] = ManagedChannelBuilder.forAddress(ips[index], Integer.parseInt(ports[index])).usePlaintext(true).build();
                stubs[index] = KeyValueStoreGrpc.newBlockingStub(channels[index]);
            }

            inFlightRequest[index] += 1;
            mu.unlock();

            Kvstore.VoteResponse response = this.stubs[index].withDeadlineAfter(RPC_TIMEOUT, TimeUnit.MILLISECONDS).requestVote(voteBuilder.build());

            mu.lock();
            if(response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm(), index);
            } else if( response.getVoteGranted() ){

                numOfVotesReceived++;
            }

        } catch (Exception e) {
            System.err.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" Exception happens in sendRequestVote to "+index+" Exception: "+e.toString());
        } finally {
            inFlightRequest[index] -= 1;
            mu.unlock();
        }


    }

    public void serverRun() {
        
        try {
            Thread.sleep(STARTUP_OVERHEAD_TIME);
        } catch (Exception e) {
            System.err.println(e.toString());
        }

        Thread applyThread = new Thread(){
            @Override
            public void run(){
                tryApply();
            }

        };
        applyThread.start();

        resetTimeout(true);
        while (true) {
            if (role.equals(RaftRole.LEADER)) {
                leaderRun();
            } else if (role.equals(RaftRole.CANDIDATE)) {
                candidateRun();
            } else {
                followerRun();
            }
        }

    }

    public void allServerRun() {
        try{
            mu.lock();
            if(commitIndex > lastApplied) {
                lastApplied++;
                String key = logs.get(lastApplied).getPutReq().getKey();
                String value = logs.get(lastApplied).getPutReq().getValue();
                kvMap.put(key, value);
            }
        } finally {
            mu.unlock();
        }

    }

    public void leaderRun() {

        while (role.equals(RaftRole.LEADER)) {
            for (int i = 0; i < numOfServers; i++) {
                final int index = i;
                if (inFlightRequest[i] >= 4) {
                    continue;
                }
                if (i != serverIndex && role.equals(RaftRole.LEADER)) {
                    Callable<Void> task = () -> {
                        sendAppendEntry(index);
                        return null;
                    };
                    executor.submit(task);
                }
            }

            updateLeaderCommit();

            try {
                Thread.sleep(HEARTBEAT_INTERVAL);
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
            }


        }
    }

    public void updateLeaderCommit() {
        int N = 0;
        boolean firstTime = true;
        while (role.equals(RaftRole.LEADER)) {
            try {
                mu.lock();
                if(firstTime) {
                    N = logs.size() -1;
                    firstTime = false;
                }
                if (!(N > commitIndex && logs.get(N).getTerm() == currentTerm)) {
                    break;
                }
                int matchCount = 1;
                for (int i = 0; i < numOfServers; i++) {
                    if (i != serverIndex) {
                        if (matchIndex[i] >= N) {
                            matchCount += 1;
                        }
                    }
                }
                if (matchCount > numOfServers / 2) {
                        commitIndex = N;
                        break;
                }
            } finally {
                N -= 1;
                mu.unlock();
            }
        }
    }

    public void followerRun() {

        /*
         *If election timeout elapses without receiving AppendEntries
         *RPC from current leader or granting vote to candidate:
         *convert to candidate
         */
        while (role.equals(RaftRole.FOLLOWER)) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL);
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+e.toString());
            }
            if(!role.equals(RaftRole.FOLLOWER))
                break;

            if (timeout <= System.currentTimeMillis() && votedFor == -1) {
                // convert to candidate
                becomeCandidate();
            }
        }
    }

    /* must acquire mu lock */
    public void becomeFollower(int term, int leader) {

        currentTerm = term;
        role = RaftRole.FOLLOWER;
        votedFor = -1;
        if(leader != leaderId) {
            System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" becomes follower for leader " + leader + ", get to term " + term);
            leaderId = leader;
            hasPersistentChanged = true;
        }
        resetTimeout(true);

    }

    public void becomeLeader() {
        System.out.println(new Timestamp((new Date()).getTime())+": server " + serverIndex + " becomes leader");
        try {
            mu.lock();
            role = RaftRole.LEADER;
            votedFor = -1;
            hasPersistentChanged = true;
            leaderId = serverIndex;
            for (int i = 0; i < numOfServers; i++) {
                nextIndex[i] = logs.size();
                matchIndex[i] = -1;
            }
        } finally {
            mu.unlock();
        }
    }


    public void becomeCandidate() {

        System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" becomes candidate");
        try {
            mu.lock();
            role = RaftRole.CANDIDATE;
            currentTerm++;
            numOfVotesReceived = 1;
            votedFor = serverIndex;
            hasPersistentChanged = true;
            /* reset election timer */
            resetTimeout(true);
        } finally {
            mu.unlock();
        }
    }

    public void candidateRun() {
        for (int i = 0; i < numOfServers; i++) {
            final int index = i;
            if (inFlightRequest[i] >= 4) {
                continue;
            }
            if (i != serverIndex && role.equals(RaftRole.CANDIDATE)) {
                Callable<Void> task = () -> {
                    sendRequestVote(index);
                    return null;
                };
                executor.submit(task);
            }
        }

        while (role.equals(RaftRole.CANDIDATE)) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL);
            } catch (Exception e) {
                System.err.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" " +e.toString());
            }

            if (!role.equals(RaftRole.CANDIDATE)) {
                break;
            }
            if (numOfVotesReceived > numOfServers / 2) {
                becomeLeader();
                break;
            } else {
                if (System.currentTimeMillis() > timeout) {
                    becomeCandidate();
                    break;
                }
            }
        }
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }


    public static void main(String args[]) throws Exception {
        
        boolean printLogger = false;
        if(args.length < 2) {
            System.err.println("Please specify server id and config file. Exit now");
            System.exit(0);
        }
        if(args[0].equals("-h") || args[0].equals("--help")) {
            System.out.println("Usage java -jar target.jar <server id> [OPTION]");
            System.out.println("-l  --logger      print stdout and stderr to logger files");
            System.out.println("-h  --help        help with user arguments");

            System.exit(0);
        }
        int index = Integer.parseInt(args[0]);
        String serverConfig = args[1];
        if(args.length == 3 && (args[2].equals("-l") || args[2].equals("--logger"))) {
            printLogger = true;
        }
        File file = new File(serverConfig);
        BufferedReader br = new BufferedReader(new FileReader(file));
        int numOfServers = Integer.parseInt(br.readLine());
        String st;
        String[] ips = new String[numOfServers];
        String[] ports = new String[numOfServers];
        for (int i = 0; i < numOfServers; i++) {
            st = br.readLine();
            String[] sp = st.split(":");
            ips[i] = sp[0];
            ports[i] = sp[1];
        }
        // now read config paramters
        while((st = br.readLine()) != null) {
            String[] sp = st.split(" ");
            if (sp.length != 2) {
                break;
            }
            int value = Integer.parseInt(sp[1]);
            switch (sp[0]) {
                case "max_num_threads":
                    MAX_NUM_THREADS = value;
                    break;
                case "election_timeout":
                    MIN_ELECTION_TIMEOUT = value;
                    break;
                case "rpc_timeout":
                    RPC_TIMEOUT = value;
                    break;
                case "heartbeat_interval":
                    HEARTBEAT_INTERVAL = value;
                    break;
                default:
                    System.out.println("Invalid config file");
            }
        }

        KvServer server = new KvServer(ips, ports, index, printLogger);
        server.start(MAX_RPC_THREADS);
        System.out.println(new Timestamp((new Date()).getTime())+": "+"grpc server started");
        server.blockUntilShutdown();
    }


    class KeyValueStoreImpl extends KeyValueStoreGrpc.KeyValueStoreImplBase {
        @Override
        public void getServerState(kvstore.Kvstore.EmptyRequest request,
                                   io.grpc.stub.StreamObserver<kvstore.Kvstore.StateResponse> responseObserver) {
            Kvstore.StateResponse.Builder responseBuilder = Kvstore.StateResponse.newBuilder();
            responseBuilder.setCommitIndex(commitIndex);
            responseBuilder.setLastApplied(lastApplied);
            responseBuilder.setLogLength(logs.size());
            responseBuilder.setRole(role.getValue());
            responseBuilder.setTerm(currentTerm);
            responseBuilder.setVoteFor(votedFor);
            responseBuilder.setServerId(serverIndex);
            responseBuilder.setLeaderId(leaderId);


            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void get(kvstore.Kvstore.GetRequest request,
                        io.grpc.stub.StreamObserver<kvstore.Kvstore.GetResponse> responseObserver) {
            System.out.println(new Timestamp((new Date()).getTime())+": "+"server "+serverIndex+" received get request");
            Kvstore.GetResponse.Builder responseBuilder = Kvstore.GetResponse.newBuilder();
            if (!kvMap.containsKey(request.getKey())) {
                responseBuilder.setRet(Kvstore.ReturnCode.FAILURE);
            } else {
                responseBuilder.setRet(Kvstore.ReturnCode.SUCCESS);
                responseBuilder.setValue(kvMap.get(request.getKey()));
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void put(kvstore.Kvstore.PutRequest request,
                        io.grpc.stub.StreamObserver<kvstore.Kvstore.PutResponse> responseObserver) {
            System.out.println(new Timestamp((new Date()).getTime())+": "+"received put request");
            int fromServer = request.getServerIndex();
            if (fromServer != -1) {
                double chaos = chaosMatrix[fromServer][serverIndex];
                Random random = new Random();
                double prob = random.nextDouble();
                if (prob < chaos) { // drop the request
                    return;
                }
            }

            Kvstore.PutResponse response = null;
            long timeoutMs = RPC_TIMEOUT;
            if (!role.equals(RaftRole.LEADER)) {
                try {
                    if(leaderId < 0) {
                        Kvstore.PutResponse.Builder responseBuilder = Kvstore.PutResponse.newBuilder();
                        responseBuilder.setRet(Kvstore.ReturnCode.FAILURE);
                        response = responseBuilder.build();
                    } else {
                        // redirect to leader.
                        if (channels[leaderId].isShutdown() || channels[leaderId].isTerminated()) {
                            channels[leaderId] = ManagedChannelBuilder.forAddress(ips[leaderId], Integer.parseInt(ports[leaderId])).usePlaintext(true).build();
                            stubs[leaderId] = KeyValueStoreGrpc.newBlockingStub(channels[leaderId]);
                        }
                        Kvstore.PutRequest.Builder reqBuilder = request.toBuilder();
                        reqBuilder.setServerIndex(serverIndex);
                        response = stubs[leaderId].withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS).put(reqBuilder.build());
                    }
                } catch (Exception e) {
                    response = Kvstore.PutResponse.newBuilder().setRet(Kvstore.ReturnCode.FAILURE).build();
                    System.err.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" failed to call put rpc" +e.toString());
                } finally {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            } else {
                Kvstore.Log.Builder logBuilder = Kvstore.Log.newBuilder();
                logBuilder.setPutReq(request);
                logBuilder.setTerm(currentTerm);
                int appendIdx = 0;
                try {
                    mu.lock();
                    //System.out.println("trying to append to local entries");
                    logs.add(logBuilder.build());
                    //System.out.println("append local done");
                    appendIdx = logs.size() - 1;
                    hasPersistentChanged = true;
                } finally {
                    mu.unlock();
                }
                int count = 0;
                Kvstore.PutResponse.Builder responseBuilder = Kvstore.PutResponse.newBuilder();
                responseBuilder.setRet(Kvstore.ReturnCode.FAILURE);
                while (role.equals(RaftRole.LEADER) && count <= 30) {
                    count += 1;
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        System.err.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" " +e.toString());
                    }
                    if (appendIdx <= lastApplied) {
                        responseBuilder.setRet(Kvstore.ReturnCode.SUCCESS);
                        break;
                    }
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void broadcast(kvstore.Kvstore.BroadcastRequest request,
                              io.grpc.stub.StreamObserver<kvstore.Kvstore.PutResponse> responseObserver) {
            Kvstore.PutResponse.Builder responseBuilder = Kvstore.PutResponse.newBuilder();
            double chaos = chaosMatrix[request.getRow()][serverIndex];
            Random random = new Random();
            double prob = random.nextDouble();
            if (prob < chaos) { // drop the request
                return;
            }

            kvMap.put(request.getKey(), request.getValue());
            responseBuilder.setRet(Kvstore.ReturnCode.SUCCESS);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }



        @Override
        public void appendEntries(kvstore.Kvstore.AppendRequest request,
                                  io.grpc.stub.StreamObserver<kvstore.Kvstore.AppendResponse> responseObserver) {
            Kvstore.AppendResponse.Builder responseBuilder = Kvstore.AppendResponse.newBuilder();
            double chaos = chaosMatrix[request.getLeaderId()][serverIndex];
            Random random = new Random();
            double prob = random.nextDouble();
            // chaosMonkey
            if (prob < chaos) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+"server "+ serverIndex+" drop appendEntries from server " + request.getLeaderId());
                return;
            }
            try {
                mu.lock();
                if (request.getTerm() >= currentTerm) {
                    becomeFollower(request.getTerm(), request.getLeaderId());
                } 
                if (!request.getIsHeartbeat()) {
                    System.out.println(new Timestamp((new Date()).getTime())+": follower " + serverIndex + " is receiving append entries grpc call from leader " + request.getLeaderId());
                }
                responseBuilder.setTerm(currentTerm);

                if (request.getTerm() < currentTerm) {
                    responseBuilder.setSuccess(false);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                int prevLogIndex = request.getPrevLogIndex();
                int prevLogTerm = request.getPrevLogTerm();
                if (logs.size() <= prevLogIndex) {
                    responseBuilder.setSuccess(false);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }

                if (prevLogIndex == -1) {

                    //logs = logs.subList(0, prevLogIndex+1);
                    logs = new CopyOnWriteArrayList(logs.subList(0, prevLogIndex+1));
                    responseBuilder.setSuccess(true);
                    if (!request.getIsHeartbeat()) {
                        logs.add(request.getEntries());
                        hasPersistentChanged = true;
                    }

                } else { // prevLogIndex >= 0
                    if (logs.get(prevLogIndex).getTerm() != prevLogTerm) {
                        responseBuilder.setSuccess(false);
                        //logs = logs.subList(0, prevLogIndex);
                        logs = new CopyOnWriteArrayList(logs.subList(0, prevLogIndex));
                        hasPersistentChanged = true;
                        responseObserver.onNext(responseBuilder.build());
                        responseObserver.onCompleted();
                        return;
                    } else { // append
                        responseBuilder.setSuccess(true);
                        //logs = logs.subList(0, prevLogIndex+1);
                        logs = new CopyOnWriteArrayList(logs.subList(0, prevLogIndex+1));
                        if (!request.getIsHeartbeat()) {
                            logs.add(request.getEntries());
                        }
                        hasPersistentChanged = true;
                    }
                }

                if (request.getLeaderCommit() > commitIndex) {
                    commitIndex = Math.min(request.getLeaderCommit(), logs.size()-1);
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            } finally {
                mu.unlock();
            }
        }

        @Override
        public void requestVote(kvstore.Kvstore.VoteRequest request,
                                io.grpc.stub.StreamObserver<kvstore.Kvstore.VoteResponse> responseObserver) {
             Kvstore.VoteResponse.Builder responseBuilder = Kvstore.VoteResponse.newBuilder();

            double chaos = chaosMatrix[request.getCandidateId()][serverIndex];
            Random random = new Random();
            double prob = random.nextDouble();
            // chaosMonkey
            if (prob < chaos) {
                System.err.println(new Timestamp((new Date()).getTime())+": "+"server "+serverIndex+" drop requestVote from server " + request.getCandidateId());
                return;
            }
            try{
                System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" receive request vote from candidate " + request.getCandidateId());
                mu.lock();
                if (request.getTerm() > currentTerm) {
                    //becomeFollower(request.getTerm(), request.getCandidateId());
                    becomeFollower(request.getTerm(), -1);
                }

                int endLogIndex = logs.size() - 1;
                responseBuilder.setTerm(currentTerm);
                if(request.getTerm() < currentTerm) {
                    responseBuilder.setVoteGranted(false);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                } else if ((votedFor < 0 || votedFor == request.getCandidateId())
                        && (request.getLastLogIndex() >= logs.size()-1 && (logs.size() == 0 || logs.get(endLogIndex).getTerm() <= request.getLastLogTerm()))) {
                    responseBuilder.setVoteGranted(true);
                    //leaderId = request.getCandidateId();
                    votedFor = request.getCandidateId();
                    leaderId = request.getCandidateId();
                    hasPersistentChanged = true;
                    System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" vote granted for candidate " + request.getCandidateId());

                } else {
                    responseBuilder.setVoteGranted(false);
                    System.out.println(new Timestamp((new Date()).getTime())+": server "+serverIndex+" vote rejected for candidate " + request.getCandidateId());
                }
                // TODO: THE FOLLOWING TWO LINE NEED DISCUSSION
                //becomeFollower(request.getTerm(), request.getCandidateId());

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();

            } finally {
                mu.unlock();
            }

        }
    }

        class ChaosMonkeyImpl extends ChaosMonkeyGrpc.ChaosMonkeyImplBase {

        @Override
        public void uploadMatrix(chaosmonkey.Chaosmonkey.ConnMatrix request,
            io.grpc.stub.StreamObserver<chaosmonkey.Chaosmonkey.Status> responseObserver) {
            chaosmonkey.Chaosmonkey.Status.Builder responseBuilder = chaosmonkey.Chaosmonkey.Status.newBuilder();
            System.out.println(new Timestamp((new Date()).getTime())+": "+"server "+serverIndex+" receive uploadMatrix");
            if(request == null) {
                responseBuilder.setRet(chaosmonkey.Chaosmonkey.StatusCode.ERROR);
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
                return;
            }

            for(int i=0; i<numOfServers; i++) {

                chaosmonkey.Chaosmonkey.ConnMatrix.MatRow matRow = request.getRows(i);
                if(null == matRow) {
                    responseBuilder.setRet(chaosmonkey.Chaosmonkey.StatusCode.ERROR);
                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                    return;
                }
                for(int j=0; j < numOfServers; j++) {
                    chaosMatrix[i][j] = (float) matRow.getVals(j);
                }
            }
            // TODO: remove the following part. test only.
            for(int i=0; i< chaosMatrix.length; i++){
                for(int j=0; j<chaosMatrix[0].length; j++) {
                    System.out.print(chaosMatrix[i][j]+" ,");
                }
                System.out.println(" ");
            }
            responseBuilder.setRet(chaosmonkey.Chaosmonkey.StatusCode.OK);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        }

        @Override
        public void updateValue(chaosmonkey.Chaosmonkey.MatValue request,
            io.grpc.stub.StreamObserver<chaosmonkey.Chaosmonkey.Status> responseObserver) {
            chaosmonkey.Chaosmonkey.Status.Builder responseBuilder = chaosmonkey.Chaosmonkey.Status.newBuilder();
            System.out.println(new Timestamp((new Date()).getTime())+": "+"receive updateValue");
            int row = request.getRow();
            int col = request.getCol();
            float val = request.getVal();
            if(row < numOfServers && row >= 0 && col < numOfServers && col >= 0 && val >= 0 && val <= 1) {
                chaosMatrix[row][col] = val;
                responseBuilder.setRet(chaosmonkey.Chaosmonkey.StatusCode.OK);
                System.out.println("Update Matrix "+row+", "+col+" to "+val);

            } else {
                responseBuilder.setRet(chaosmonkey.Chaosmonkey.StatusCode.ERROR);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getMatrix(chaosmonkey.Chaosmonkey.EmptyRequest request,
                              io.grpc.stub.StreamObserver<chaosmonkey.Chaosmonkey.ConnMatrix> responseObserver) {
            Chaosmonkey.ConnMatrix.Builder responseBuilder = Chaosmonkey.ConnMatrix.newBuilder();
            int rows = chaosMatrix.length;
            int cols = rows;
            for (int i = 0; i < rows; i++) {
                chaosmonkey.Chaosmonkey.ConnMatrix.MatRow.Builder rowBuilder = chaosmonkey.Chaosmonkey.ConnMatrix.MatRow.newBuilder();
                for (int j = 0; j < cols; j++) {
                    rowBuilder.addVals(chaosMatrix[i][j]);
                }
                responseBuilder.addRows(rowBuilder.build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }



}
