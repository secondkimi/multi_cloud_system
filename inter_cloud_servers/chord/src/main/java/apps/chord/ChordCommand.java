package apps.chord;

import grpc.chord.Chord;
import grpc.chord.Stat;
import grpc.chord.StatStoreGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import grpc.chord.ChordServerStoreGrpc;

public class ChordCommand {

    public  int M = 32;
    public String statMgrIp = "127.0.0.1";
    public int statMgrPort = 8888;
    public String chordServerIp;
    public int chordServerPort;
    private static final String OUTFILE_NAME = "stats.txt";
    private static final String COMMAND_TYPES = "Supported commands are: getStats, joinRing, leaveRing and putKey";
    private static final String DEFAULT_KEY_AMOUNT = "1000";
    private static final String PUTKEY_HELPER_STR = "PutKey <number_of_keys>";
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    //private String command;
    private ChordNode[] chordNodes;
    public static int totalPuts = 0;
    public SortedMap<Long, String> localDataMap;
    public Map<ChordNode, ManagedChannel> channelMap;


    public ChordCommand(ChordNode[] chordNodes) {
        //this.command = command;
        this.chordNodes = chordNodes;
        localDataMap = new TreeMap<>();
        channelMap = new ConcurrentHashMap<>();
    }


    public void execute(String command) {
        if(command == null || command.equals("")) return;
        String[] commandTokens = command.split(" ");
        switch (commandTokens[0].toLowerCase()) {
            case "quit":
                System.exit(0);
            case "exit":
                System.exit(0);
            case "getstats":
                cmdGetStats();
                break;
            case "joinring":
                if(commandTokens.length == 3) {
                    cmdJoinRing(commandTokens[1], commandTokens[2]);
                    
                } else {
                    System.out.println("joinRing <from_server_index> <to_server_index>");
                }
                break;
            case "leavering":
                if(commandTokens.length >= 2) {
                    cmdLeaveRing(commandTokens[1]);
                    
                } else {
                    System.out.println("leaveRing <server_index>");
                }
                break;
            case "putkey":
                if(commandTokens.length == 2) {
                    cmdPutKey(commandTokens[1]);
                } else {
                    cmdPutKey(DEFAULT_KEY_AMOUNT);
                }
                break;
            default:
                System.out.println(COMMAND_TYPES);
                break;
        } 
        System.out.print("> ");

    }

    public String generateString(int length) {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            Random random = new Random();
            int idx = random.nextInt(ALPHABET.length());
            strBuilder.append(ALPHABET.charAt(idx));
        }
        return strBuilder.toString();
    }

    
    public void putRandomKey(Map<Integer, Integer> pathMap) {
        try{
            Random random = new Random();
            int keyLength = random.nextInt(32) + 32;
            int dataLength = random.nextInt(32) + 32;
            String key = generateString(keyLength);
            String data = generateString(dataLength);
            int server_idx; 
            do {
                server_idx = random.nextInt(chordNodes.length);
            }while(!chordNodes[server_idx].alive);
                
            String ip = chordNodes[server_idx].get_ip();
            int port = chordNodes[server_idx].get_m_port();
            ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext(true).build();
            ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);
            Chord.IdInfo.Builder idInfoBuilder = Chord.IdInfo.newBuilder();
            long id = ChordHashing.hash_key(key, M);

            localDataMap.put(id, data);
            //System.out.println("id : " + Long.toHexString(id));
            idInfoBuilder.setId(id);
            Chord.NodeInfo nodeInfo = null;
            
            nodeInfo = stub.getSuccessor(idInfoBuilder.build());
            channel.shutdown();
            if(nodeInfo == null || nodeInfo.getId() == -1) {
                System.out.println("Failed to get successor");
                return;
            }
            // put path length on statics manager.
            int pathLength = nodeInfo.getPathLength();
            pathMap.put(pathLength, pathMap.getOrDefault(pathLength, 0)+1);
            
            // put data in chord node.

            ip = nodeInfo.getIp();
            port = nodeInfo.getPort();
            channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext(true).build();
            stub = ChordServerStoreGrpc.newBlockingStub(channel);
            Chord.Data.Builder dataBuilder = Chord.Data.newBuilder();
            dataBuilder.setId(id);
            dataBuilder.setData(data);

            Chord.RetCode  retCode = stub.putData(dataBuilder.build());
            //channel.awaitTermination(150, TimeUnit.MILLISECONDS);
            
            if(retCode.getIsSuccess()) {
                totalPuts += 1;
                System.out.println( totalPuts+" puts already returned");
            } 

            channel.shutdown();
	    
            
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

        
    private void notifyPutToStatMgr(Map<Integer, Integer> hashMap) {
        Stat.PathLengthList.Builder addKeyListBuilder = Stat.PathLengthList.newBuilder();
        hashMap.forEach((pathLen, count) -> {
            Stat.PathLengthInfo.Builder pathLenInfoBuilder = Stat.PathLengthInfo.newBuilder();
            pathLenInfoBuilder.setPathLength(pathLen);
            pathLenInfoBuilder.setPathCount(count);
            addKeyListBuilder.addPathLengthInfo(pathLenInfoBuilder.build()); 
        });

        int times = 3;
        ManagedChannel channel2 = ManagedChannelBuilder.forAddress(statMgrIp, statMgrPort).usePlaintext(true).build();
        StatStoreGrpc.StatStoreBlockingStub statStub = StatStoreGrpc.newBlockingStub(channel2);
        while(times-- > 0) {
            if(channel2.isTerminated() || channel2.isShutdown()) {
                channel2 = ManagedChannelBuilder.forAddress(statMgrIp, statMgrPort).usePlaintext(true).build();
                statStub = StatStoreGrpc.newBlockingStub(channel2);
            }
            Stat.SimpleResponse simpleResponse = statStub.notifyAddListOfKeys(addKeyListBuilder.build());
            try{
                channel2.awaitTermination(1500, TimeUnit.MILLISECONDS);
            } catch(Exception e) {
                System.out.println(e.toString());
            } 
            
            if(simpleResponse.getRet() == Stat.ReturnCode.SUCCESS) {
                break;
            } else if(times == 0) {
                System.out.println("triple timeout in putRandomKey() notifyAddKey grpc");
                channel2.shutdown();
                return;
            }
        }
        channel2.shutdown();
    }

    public void cmdPutKey(String amount_str) {
       
        System.out.println("start putting data on the ring.");
        int putAmount = Integer.parseInt(amount_str);
        if(putAmount <= 0) {
            System.out.println(PUTKEY_HELPER_STR);
            return;
        }
        Map<Integer, Integer> pathMap = new ConcurrentHashMap<>(); 
        //Map<ChordNode, Map<Long, String>> dataMap = new ConcurrentHashMap<ChordNode, Map<Long, String>>();
  
        int MAX_THREAD = 100;
        int amountLeft = putAmount;
        while(amountLeft > 0) {
            int threadCount = amountLeft;
            if(amountLeft > MAX_THREAD) {
              threadCount = MAX_THREAD;
              amountLeft -= MAX_THREAD;  
            }  else {
                amountLeft = 0;
            }
   
            Thread[] putThreads = new Thread[threadCount];
            try{
                for (int i = 0; i < threadCount; i++) {
                    putThreads[i] = new PutThread(pathMap);
                    putThreads[i].start();   
                }
                for (int i = 0; i < threadCount; i++) {
                    putThreads[i].join();    
                }
            } catch(Exception e) {
                System.out.println(e.toString());
            }

        }
        
        
        notifyPutToStatMgr(pathMap);
        //sendDataToServers(dataMap);
        System.out.println("put data done.");
        
    }

    public void cmdLeaveRing(String leave_index_str) {
        int leave_index = 0;
        try{

            leave_index = Integer.parseInt(leave_index_str);
            if(leave_index >= chordNodes.length || leave_index < 0) {
                System.out.println("server index does not exist");
                return;
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        ChordNode leaveNode = chordNodes[leave_index];
        
        ManagedChannel chordChannel;
        ChordServerStoreGrpc.ChordServerStoreBlockingStub chordStub;
        chordChannel = ManagedChannelBuilder.forAddress(leaveNode.get_ip(), leaveNode.get_m_port()).usePlaintext(true).build();
        chordStub = ChordServerStoreGrpc.newBlockingStub(chordChannel);
        Chord.MigrationData migrationData = chordStub.leaveRing(Chord.Ping.newBuilder().build());
        Chord.DataList dataList = migrationData.getDataList();
        Chord.NodeInfo successorInfo = migrationData.getSuccessor();
        chordNodes[leave_index].alive = false;
        System.out.println("Migrating "+dataList.getDataListList().size() +" data to "+successorInfo.getIp()+":"+successorInfo.getPort());
        for(Chord.Data data : dataList.getDataListList()) {
            System.out.println("Id: "+data.getId() +"Data: "+ data.getData());
        }

    }


    public void cmdJoinRing(String from_index_str, String to_index_str) {
        int from_index = 0;
        int to_index = 0;
        try{

            from_index = Integer.parseInt(from_index_str);
            to_index = Integer.parseInt(to_index_str);
            if(from_index == to_index || from_index >= chordNodes.length || to_index >= chordNodes.length) {
                System.out.println("server that calls join, index "+from_index +" does not exist");
                return;
            }
            if(!chordNodes[to_index].alive) {
                System.out.println("server to join, index "+to_index +" ID: "+chordNodes[to_index].get_my_id()+" is now dead");
                return;
            }

        } catch (Exception e) {
            System.out.println(e.toString());
        }
        ChordNode fromNode = chordNodes[from_index];
        ChordNode toNode = chordNodes[to_index];
        
        ManagedChannel chordChannel;
        ChordServerStoreGrpc.ChordServerStoreBlockingStub chordStub;
        chordChannel = ManagedChannelBuilder.forAddress(fromNode.get_ip(), fromNode.get_m_port()).usePlaintext(true).build();
        chordStub = ChordServerStoreGrpc.newBlockingStub(chordChannel);
        Chord.NodeInfo.Builder reqBuilder = Chord.NodeInfo.newBuilder();
        reqBuilder.setIp(toNode.get_ip());
        reqBuilder.setPort(toNode.get_m_port());
        reqBuilder.setId(toNode.get_my_id());
        Chord.MigrationData migrationData = chordStub.joinRing(reqBuilder.build());
        Chord.DataList dataList = migrationData.getDataList();
        Chord.NodeInfo successorInfo = migrationData.getSuccessor();
        chordNodes[from_index].alive = true;
        System.out.println("Migrating "+dataList.getDataListList().size() +" data from "+successorInfo.getIp()+":"+successorInfo.getPort());
        for(Chord.Data data : dataList.getDataListList()) {
            System.out.println("Id: "+data.getId() +"Data: "+ data.getData());
        }

    }

    public void cmdGetStats() {
        System.out.println("getting stats");
        ManagedChannel statChannel;
        StatStoreGrpc.StatStoreBlockingStub statStub;
        statChannel = ManagedChannelBuilder.forAddress(statMgrIp, statMgrPort).usePlaintext(true).build();
        statStub = StatStoreGrpc.newBlockingStub(statChannel);
        Stat.StaticsResult statics = statStub.getStatics(Stat.Ping1.newBuilder().build());
        try
        {
            PrintWriter writer = new PrintWriter(OUTFILE_NAME);
            writer.println("Node Stat Info:");
            int totalServers = 0;
            int keys = 0;
            for(Stat.NodeStatInfo nodeInfo : statics.getNodeInfosList()) {
                long id = nodeInfo.getId();
                int numKeys = nodeInfo.getNumOfKeys();
                writer.println("ID: "+id+" Number of Keys: "+numKeys);
                System.out.println("ID: "+id+" Number of Keys: "+numKeys);
                keys += numKeys;
                totalServers++;
            }
            System.out.println("Total number of servers: "+totalServers);
            writer.println("Total number of servers: "+totalServers);
            writer.println("Path Length Info:");
            for(Stat.PathLengthInfo pathInfo : statics.getLengthInfosList()) {
                int pathLen = pathInfo.getPathLength();
                int pathCount = pathInfo.getPathCount();
                writer.println("Path Length: "+pathLen+" Path Count: "+pathCount);
                System.out.println("Path Length: "+pathLen+" Path Count: "+pathCount);

            }
            System.out.println("Total key in: "+keys);
            writer.println("Total key in: "+keys);
            writer.close();
            statChannel.shutdown();

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    class PutThread extends Thread {
        protected Map<Integer, Integer> pathMap;
        public PutThread(Map<Integer, Integer> pathMap) {
            this.pathMap = pathMap;
        }

        @Override
        public void run() {
            putRandomKey(pathMap);
            
        }
    }  // end inner class


}
