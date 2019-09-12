package apps.chord;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Map;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import grpc.chord.*;

public class ChordNode {

    /* Define some constants */
    //private static final int M = 32;
    private static String statMgrIp = "127.0.0.1";
    private static int statMgrPort = 8888;
    private String m_ip;
    private int m_port;
    private long my_id;
    private ChordNode[] fingers;
    private static final int M = 32;


    public Map<Long, String> dataMap;
    public ChordNode predecessor;
    public ChordNode successor;
    public boolean alive = false;
    public boolean uploadStat = false;
    public Map<ChordNode, ManagedChannel> channelMap;

    public ChordNode(String ip, int port) {
        this(ip, port, false);
    }

    public ChordNode(String ip, int port, boolean alive) {
        m_ip = ip;
        m_port = port;
        my_id = ChordHashing.hash_ip(ip, port, M);
        fingers = new ChordNode[M];
        dataMap = new ConcurrentSkipListMap<>();
        this.alive = alive;
        channelMap = new ConcurrentHashMap<>();
    }

    /*
     * join a Chord ring containing node.
     */
    public Chord.DataList join(ChordNode node) {
        while(true) {
            Chord.NodeInfo successorInfo = find_successor_grpc(node, my_id, false);
            if(successorInfo != null) {
                successor = new ChordNode(successorInfo.getIp(), successorInfo.getPort());
                break;
            }
        }
        
        predecessor = null;
        System.out.println("joining node "+node.get_ip()+":"+node.get_m_port());

        //ManagedChannel channel = ManagedChannelBuilder.forAddress(successor.get_ip(), successor.get_m_port()).usePlaintext(true).build();
        //ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);
        ManagedChannel channel = channelMap.get(successor);
        if(channel == null || channel.isTerminated() || channel.isShutdown()) {
            channel = ManagedChannelBuilder.forAddress(successor.get_ip(), successor.get_m_port()).usePlaintext(true).build();
            channelMap.put(successor, channel);
        }
        ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);

        
        Chord.NodeInfo.Builder myInfoBuilder = Chord.NodeInfo.newBuilder();
        myInfoBuilder.setId(my_id);
        myInfoBuilder.setIp(m_ip);
        myInfoBuilder.setPort(m_port);
        Chord.DataList redistData = stub.notifyAndGetData(myInfoBuilder.build());
        /*
        channel.shutdown();
        try {
            channel.awaitTermination(1500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println(e);
        }*/
        for(Chord.Data data : redistData.getDataListList()) {
            
            dataMap.put(data.getId(), data.getData());
        }
        uploadStat = true;
        alive = true;
        return redistData;
        
    }

    /*
     * create a new ring
     */
    public void create() {
        predecessor = null;
        successor = new ChordNode(m_ip, m_port);
        System.out.println("creating node "+this.get_ip()+":"+this.get_m_port());
        alive = true;
    }

    // return true if middle in (left, right)
    public boolean is_in_between_exclusive(long left, long middle, long right) {
        if (right > left) {
            if (middle < right && middle > left) {
                return true;
            }
        } else {
            if (middle < right || middle > left) {
                return true;
            }
        }
        return false;
    }

    // return true in middle in (left, right]
    public static boolean is_in_between_right_inclusive(long left, long middle, long right) {
        if (right > left) {
            if (middle <= right && middle > left) {
                return true;
            }
        } else {
            if (middle <= right || middle > left) {
                return true;
            }
        }
        return false;
    }

    /*
     * called periodically. verifies n's immediate successor,
     * and tells the successor about n.
     */
    public void stabilize() {
        ManagedChannel channel = channelMap.get(successor);
        if(channel == null || channel.isTerminated() || channel.isShutdown()) {
            channel = ManagedChannelBuilder.forAddress(successor.get_ip(), successor.get_m_port()).usePlaintext(true).build();
            channelMap.put(successor, channel);
        }
        ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);
        Chord.IdInfo.Builder reqBuilder = Chord.IdInfo.newBuilder();
        Chord.NodeInfo predecessorInfo = stub.getPredecessor(Chord.Ping.newBuilder().build());
        /*
        channel.shutdown();
        try {
            channel.awaitTermination(1500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println(e);
        }*/
        if (predecessorInfo.getId() == -1) {
            return;
        }
        boolean successorChanged = false;
        if(is_in_between_exclusive(my_id, predecessorInfo.getId(), successor.get_my_id())) {
            successor = new ChordNode(predecessorInfo.getIp(), predecessorInfo.getPort());
            successorChanged = true;
        }
        if (successorChanged) {
            channel = channelMap.get(successor);
            if(channel == null || channel.isTerminated() || channel.isShutdown()) {
                channel = ManagedChannelBuilder.forAddress(successor.get_ip(), successor.get_m_port()).usePlaintext(true).build();
                channelMap.put(successor, channel);
            }
            stub = ChordServerStoreGrpc.newBlockingStub(channel);
            
            Chord.NodeInfo.Builder myInfoBuilder = Chord.NodeInfo.newBuilder();
            myInfoBuilder.setId(my_id);
            myInfoBuilder.setIp(m_ip);
            myInfoBuilder.setPort(m_port);
            Chord.DataList redistData = stub.notifyAndGetData(myInfoBuilder.build());
            /*
            channel.shutdown();
            try {
                channel.awaitTermination(1500, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                System.out.println(e);
            }*/
            for(Chord.Data data : redistData.getDataListList()) {
                dataMap.put(data.getId(), data.getData());
            }
            uploadStat = true;
        }
    }

    // n2 thinks it might be our predecessor.
    public boolean notify(ChordNode n2) {
        if (predecessor == null) {
            predecessor = n2;
            return true;
        } else {
            if(is_in_between_exclusive(predecessor.get_my_id(), n2.get_my_id(), my_id)){
                predecessor = n2;
                return true;
            }
        }
        return false;
    }

    /*
     * called periodically. refreshes finger table entries.
     * next stores the index of the next finger to fix.
     */
    public void fix_fingers() {
        for (int i = 0; i < M; i++) {
            Chord.NodeInfo nodeInfo = find_successor((my_id + (long)Math.pow(2, i)) % (long) Math.pow(2, M));
            if(nodeInfo != null && nodeInfo.getId() != -1)
                fingers[i] = new ChordNode(nodeInfo.getIp(), nodeInfo.getPort());
        }
    }

    /*
     * called periodically. checks whether predecessor has failed.
     */
    public void check_predecessor() {
        if (predecessor != null) {
            ManagedChannel channel = channelMap.get(predecessor);
            if(channel == null || channel.isTerminated() || channel.isShutdown()) {
                channel = ManagedChannelBuilder.forAddress(predecessor.get_ip(), predecessor.get_m_port()).usePlaintext(true).build();
                channelMap.put(predecessor, channel);
            }
            ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);
            Chord.Status isAlive = stub.isAlive(Chord.Ping.newBuilder().build());
            if (!isAlive.getIsAlive()) {
                predecessor = null;
            }
            /*
            channel.shutdown();
            try {
                channel.awaitTermination(1500, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                System.out.println(e);
            }*/
            
        }
    }

    // search the local table for the highest predecessor of id.
    public ChordNode closest_preceding_node(long id) {
        for (int i=M-1; i>=0; i--) {
            if (fingers[i] != null) {
                long curr_id = fingers[i].get_my_id();
                if(is_in_between_exclusive(my_id, curr_id, id)) {
                    return fingers[i];
                }
            }
        }
        return this;
    }

    // find the successor of this id, from current table.
    public Chord.NodeInfo find_successor(long id) {
        long successor_id = successor.get_my_id();
        if( is_in_between_right_inclusive(my_id, id, successor_id) ) {
            return nodeToInfo(successor);
        }

        ChordNode node = closest_preceding_node(id);
        Chord.NodeInfo successorInfo = find_successor_grpc(node, id, true);
        if(successorInfo == null) {
            //System.out.println("successor is null");
            return null;
        }
        return successorInfo;
    }

    public Chord.NodeInfo find_successor_grpc(ChordNode node, long id, boolean hasTimeout) {
        // TODO: need resend mechanism in case of timeout
        ManagedChannel channel = channelMap.get(node);
        if(channel == null || channel.isTerminated() || channel.isShutdown()) {
            channel = ManagedChannelBuilder.forAddress(node.get_ip(), node.get_m_port()).usePlaintext(true).build();
            channelMap.put(node, channel);
        }
        ChordServerStoreGrpc.ChordServerStoreBlockingStub stub = ChordServerStoreGrpc.newBlockingStub(channel);
        Chord.IdInfo.Builder reqBuilder = Chord.IdInfo.newBuilder();
        reqBuilder.setId(id);
        Chord.NodeInfo successorInfo = null;
            
        try {
            if(hasTimeout) {
                successorInfo = stub.withDeadlineAfter(5000, TimeUnit.MILLISECONDS).getSuccessor(reqBuilder.build());
                //successorInfo = stub.getSuccessor(reqBuilder.build());
            } else {
                successorInfo = stub.getSuccessor(reqBuilder.build());
            }
            
        } catch (Exception e) {
            //System.out.println(e);
        }
        /*
        if(successorInfo != null && successorInfo.getId() == -1) {
            System.out.println("Incorrect successor Id returned. this successor is alreay down");
            return null;
        }*/
        if(successorInfo != null && successorInfo.getPathLength() > 0) {
            return successorInfo;
        }
        return null;

    }

    public String get_ip() {
        return m_ip;
    }
    public int get_m_port() {
        return m_port;
    }

    public long get_my_id() {
        return my_id;
    }
    
    @Override 
    public int hashCode() {
        return (int) my_id;
    }

    @Override
    public boolean equals(Object obj) {
        ChordNode other = (ChordNode) obj;
        return m_ip.equals(other.get_ip()) && m_port == other.get_m_port(); 
    }

    public static Chord.NodeInfo nodeToInfo(ChordNode node) {
        Chord.NodeInfo.Builder builder = Chord.NodeInfo.newBuilder();
        builder.setId(node.get_my_id());
        builder.setIp(node.get_ip());
        builder.setPort(node.get_m_port());
        builder.setPathLength(0);
        return builder.build();
    }


    public void uploadStatMgr() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(statMgrIp, statMgrPort).usePlaintext(true).build();
        StatStoreGrpc.StatStoreBlockingStub statStub = StatStoreGrpc.newBlockingStub(channel);
        Stat.NodeStatInfo.Builder infoBuilder = Stat.NodeStatInfo.newBuilder();
        infoBuilder.setNumOfKeys(dataMap.size());
        infoBuilder.setId(my_id);

        statStub.updateNodeInfo(infoBuilder.build());
        channel.shutdown();
        try {
            channel.awaitTermination(1500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public void run() {
        while (true) {
            try {
                Thread.sleep(500);
                if(alive) {
                    stabilize();
                    check_predecessor();
                    if(uploadStat) {
                        uploadStatMgr();
                        uploadStat = false;
                    }
                    
                }
            } catch (Exception e) {

            }
        }
    }

    public void run_finger() {
        while (true) {
            try {
                Thread.sleep(50);
                if(alive) {
                    fix_fingers();       
                }
            } catch (Exception e) {

            }
        }
    }

}
