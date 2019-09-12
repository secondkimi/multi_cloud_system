package apps.chord;

import grpc.chord.Chord;
import grpc.chord.ChordServerStoreGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.lang.Thread;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
// used for reading config files
import java.io.FileNotFoundException; 
import java.io.FileOutputStream; 
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;

/* logging */
import java.util.Date;
import java.sql.Timestamp;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

public class ChordServer {
    ChordNode m_node;
    Server server;
    ExecutorService executor;

    public ChordServer(String ip, int port, String mgrIp, int mgrPort) {
        m_node = new ChordNode(ip, port);
    }

    public ChordServer(String ip, int port) {
        m_node = new ChordNode(ip, port);
        executor= Executors.newFixedThreadPool(10);
        Callable<Void> task = () -> {
            m_node.run();
            return null;
        };

        System.out.println("run as id: " + Long.toHexString(m_node.get_my_id()));
        executor.submit(task);
        Callable<Void> finger_task = () -> {
            m_node.run_finger();
            return null;
        };
        executor.submit(finger_task);
    }

    public static void main(String[] args) throws Exception {
        // args[0]: config file
        // args[1]: -i , --index - indicate the next arg is current server id
        // args[2]: <line_number> - which line from config file to read
        // args[3]: -j, --join - indicate the next arg is the node to join
        // args[4]: <line_number> - which line from config file to read
        // if there is no -j flag, then create a new node
        int id_line = -1;
        int join_line = -1;
        String format = "config_file -i <server_id> -j <join_server_id>";
        if(args.length < 1) {
            System.out.println(format);
            System.exit(0);
        }
        ChordServer chordServer;
        String line;
        int curr_line = 0;
        String my_ip_addr = null;
        int my_port = -1;
        String join_ip_addr = null;
        int join_port = -1;
        boolean alive = true;
        try {
            
            String serverConfig = args[0];
            if(args.length >= 3) {
                if(args[1].equals("-i") || args[1].equals("--index")) {
                    id_line = Integer.parseInt(args[2]);
                } 
            } 
            if(args.length == 5) {
                if(args[3].equals("-j") || args[3].equals("--join")) {
                    join_line = Integer.parseInt(args[4]);
                }
            }
            if(args.length == 6) {
                if(args[5].equals("-down") || args[5].equals("-d") ) {
                    alive = false;
                }  
            }
            if(id_line == -1) {
                System.out.println(format);
                System.exit(0);
            }
            File file = new File(serverConfig);
            BufferedReader br = new BufferedReader(new FileReader(file));
            
            // read the first line, which is number of servers
            if((line = br.readLine()) == null) {
                System.out.println("Wrong config file format");
                System.exit(0);
            }
            while((line = br.readLine()) != null) {
                if(curr_line == id_line) {
                    String[] sp = line.split(":");
                    my_ip_addr = sp[0];
                    my_port = Integer.parseInt(sp[1]);
                } else if(curr_line == join_line) {
                    String[] sp = line.split(":");
                    join_ip_addr = sp[0];
                    join_port = Integer.parseInt(sp[1]);
                }
                curr_line++;
            }
            br.close();

        } catch (Exception e) {
            System.out.println(e.toString());
        }

        if(my_ip_addr == null || my_port == -1) {
            System.out.println(format);
            System.exit(0);
        }
        chordServer = new ChordServer(my_ip_addr, my_port);

        chordServer.start(10);
        if(join_ip_addr == null || join_port == -1) {
            // create
            chordServer.m_node.create();

        } else if(alive) {
            // join iff it is alive.
            try{
                Thread.sleep(3000);
            } catch (Exception e) {

            } 
            chordServer.m_node.join(new ChordNode(join_ip_addr, join_port));
        }
        chordServer.blockUntilShutdown();
        

    }

    public void start(int numThreads) throws IOException {
        this.server = ServerBuilder.forPort(this.m_node.get_m_port())
                .addService(new ChordServerStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();

        System.out.println(new Timestamp((new Date()).getTime())+": server starts at port: " + this.m_node.get_m_port());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ChordServer.this.stop();
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }


    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    // RPC Server Implementation.
    class ChordServerStoreImpl extends ChordServerStoreGrpc.ChordServerStoreImplBase {
        @Override
        public void getSuccessor(grpc.chord.Chord.IdInfo request,
                                 io.grpc.stub.StreamObserver<grpc.chord.Chord.NodeInfo> responseObserver) {
            Chord.NodeInfo.Builder responseBuilder = Chord.NodeInfo.newBuilder();
//            System.out.println("received get successor request for id: " + Long.toHexString(request.getId()));

            if (m_node.alive) {
                Chord.NodeInfo nodeInfo = m_node.find_successor(request.getId());
                if(nodeInfo != null) {
                    responseBuilder.setPort(nodeInfo.getPort());
                    responseBuilder.setIp(nodeInfo.getIp());
                    responseBuilder.setId(nodeInfo.getId());
                    responseBuilder.setPathLength(nodeInfo.getPathLength()+1);
                } else {
                    responseBuilder.setId((long) -1);
                }
                
            } else {
                // notify the client I am down
                responseBuilder.setId((long) -1);
            }
//            System.out.println("successor is: " + Long.toHexString(responseBuilder.getId()));
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void notifyAndGetData(grpc.chord.Chord.NodeInfo request,
                                     io.grpc.stub.StreamObserver<grpc.chord.Chord.DataList> responseObserver) {
            System.out.println("received notifyANdGetData from : " + Long.toHexString(request.getId()));
            ChordNode n2Node = new ChordNode(request.getIp(), request.getPort());
            Chord.DataList.Builder responseBuilder = Chord.DataList.newBuilder();
            if (m_node.alive) {
                boolean changePredecessor = m_node.notify(n2Node);
                if (changePredecessor) {
//                    SortedMap<Long, String> toSendData = m_node.dataMap.subMap(0L, request.getId() + 1);
                    Map<Long, String> toSendData = new HashMap<>();
                    for (Long key : m_node.dataMap.keySet()) {
                        long left = request.getId();
                        long right = m_node.get_my_id();
                        if (!ChordNode.is_in_between_right_inclusive(left, key, right)) {
                            toSendData.put(key, m_node.dataMap.get(key));
                        }
                    }
                    for (Long key : toSendData.keySet()) {
                        m_node.dataMap.remove(key);
                        Chord.Data.Builder builder = Chord.Data.newBuilder();
                        builder.setId(key);
                        builder.setData(toSendData.get(key));
                        responseBuilder.addDataList(builder.build());
                    }
                    m_node.uploadStat = true;
                }
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getPredecessor(grpc.chord.Chord.Ping request,
                                   io.grpc.stub.StreamObserver<grpc.chord.Chord.NodeInfo> responseObserver) {
            Chord.NodeInfo.Builder resBuilder = Chord.NodeInfo.newBuilder();
            resBuilder.setId(-1);
            if (m_node.predecessor != null && m_node.alive) {
                resBuilder.setId(m_node.predecessor.get_my_id());
                resBuilder.setPort(m_node.predecessor.get_m_port());
                resBuilder.setIp(m_node.predecessor.get_ip());
            }
            responseObserver.onNext(resBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void isAlive(grpc.chord.Chord.Ping request,
                            io.grpc.stub.StreamObserver<grpc.chord.Chord.Status> responseObserver) {
            Chord.Status.Builder resBuilder = Chord.Status.newBuilder();
            resBuilder.setIsAlive(m_node.alive);
            responseObserver.onNext(resBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void joinRing(grpc.chord.Chord.NodeInfo request,
                             io.grpc.stub.StreamObserver<grpc.chord.Chord.MigrationData> responseObserver) {
            ChordNode nodeToJoin = new ChordNode(request.getIp(), request.getPort());
           

            Chord.DataList dataList = m_node.join(nodeToJoin);
            Chord.MigrationData.Builder responseBuilder = Chord.MigrationData.newBuilder();
            Chord.NodeInfo.Builder nodeInfoBuilder = Chord.NodeInfo.newBuilder();
            nodeInfoBuilder.setIp(nodeToJoin.get_ip());
            nodeInfoBuilder.setId(nodeToJoin.get_my_id());
            nodeInfoBuilder.setPort(nodeToJoin.get_m_port());
            nodeInfoBuilder.setPathLength(0);

            responseBuilder.setDataList(dataList);
            responseBuilder.setSuccessor(nodeInfoBuilder.build());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            m_node.alive = true;


        }

        @Override
        public void leaveRing(grpc.chord.Chord.Ping request,
                              io.grpc.stub.StreamObserver<grpc.chord.Chord.MigrationData> responseObserver) {

            // Notify leave to predecessor.
            Chord.NodeInfo.Builder successorInfoBuilder = Chord.NodeInfo.newBuilder();
            successorInfoBuilder.setId(-1);
            if (m_node.successor != null) {
                successorInfoBuilder.setId(m_node.successor.get_my_id());
                successorInfoBuilder.setIp(m_node.successor.get_ip());
                successorInfoBuilder.setPort(m_node.successor.get_m_port());
            }
            ManagedChannel channel;
            ChordServerStoreGrpc.ChordServerStoreBlockingStub stub;
            if (m_node.predecessor != null) {
                channel = ManagedChannelBuilder.forAddress(m_node.predecessor.get_ip(), m_node.predecessor.get_m_port()).usePlaintext(true).build();
                stub = ChordServerStoreGrpc.newBlockingStub(channel);
                stub.notifyLeaveToPredecessor(successorInfoBuilder.build());
                channel.shutdown();
            }

            // Notify Leave to successor
            Chord.NotifyLeaveData.Builder leaveDataBuilder = Chord.NotifyLeaveData.newBuilder();
            Chord.DataList.Builder dataListBuilder = Chord.DataList.newBuilder();
            for (long id : m_node.dataMap.keySet()) {
                Chord.Data.Builder dataBuilder = Chord.Data.newBuilder();
                dataBuilder.setData(m_node.dataMap.get(id));
                dataBuilder.setId(id);
                dataListBuilder.addDataList(dataBuilder.build());
            }
            leaveDataBuilder.setDataList(dataListBuilder.build());
            Chord.NodeInfo.Builder predecessorInfoBuilder = Chord.NodeInfo.newBuilder();
            predecessorInfoBuilder.setId(-1);
            if (m_node.predecessor != null) {
                predecessorInfoBuilder.setId(m_node.predecessor.get_my_id());
                predecessorInfoBuilder.setPort(m_node.predecessor.get_m_port());
                predecessorInfoBuilder.setIp(m_node.predecessor.get_ip());
            }
            leaveDataBuilder.setPredecessor(predecessorInfoBuilder.build());

            if (m_node.successor != null) {
                channel = ManagedChannelBuilder.forAddress(m_node.successor.get_ip(), m_node.successor.get_m_port()).usePlaintext(true).build();
                stub = ChordServerStoreGrpc.newBlockingStub(channel);
                stub.notifyLeaveToSuccessor(leaveDataBuilder.build());
                channel.shutdown();
            }
            m_node.dataMap = new ConcurrentSkipListMap<>();
            m_node.uploadStatMgr();
            m_node.alive = false;
            //m_node.uploadStat = true;
            Chord.MigrationData.Builder responseBuilder = Chord.MigrationData.newBuilder();
            responseBuilder.setDataList(dataListBuilder.build());
            responseBuilder.setSuccessor(successorInfoBuilder.build());
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void notifyLeaveToPredecessor(grpc.chord.Chord.NodeInfo request,
                                             io.grpc.stub.StreamObserver<grpc.chord.Chord.Pong> responseObserver) {
            System.out.println("received notifyLeaveToPredecessor from : " + Long.toHexString(request.getId()));
            if (request.getId() != -1 && request.getId() != m_node.successor.get_my_id()) {
                m_node.successor = new ChordNode(request.getIp(), request.getPort());
            } else {
                // my successor leave, but there is only myself left in the ring.
                m_node.successor = new ChordNode(m_node.get_ip(), m_node.get_m_port());
            }
            responseObserver.onNext(Chord.Pong.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void notifyLeaveToSuccessor(grpc.chord.Chord.NotifyLeaveData request,
                                           io.grpc.stub.StreamObserver<grpc.chord.Chord.Pong> responseObserver) {
            if (request.getPredecessor().getId() != -1) {
                m_node.predecessor = new ChordNode(request.getPredecessor().getIp(), request.getPredecessor().getPort());
            } else {
                // if I do not have a predcessor, set it to null
                m_node.predecessor = null;
            }

            Chord.DataList dataList = request.getDataList();
            for (Chord.Data data : dataList.getDataListList()) {
                m_node.dataMap.put(data.getId(), data.getData());
            }
            m_node.uploadStat = true;

            responseObserver.onNext(Chord.Pong.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void putData(grpc.chord.Chord.Data request,
                            io.grpc.stub.StreamObserver<grpc.chord.Chord.RetCode> responseObserver) {
            
            Long id = request.getId();
            String data = request.getData();
            m_node.dataMap.put(id, request.getData());
            m_node.uploadStat = true;
            Chord.RetCode.Builder retBuilder = Chord.RetCode.newBuilder();
            retBuilder.setIsSuccess(true);
            responseObserver.onNext(retBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void putDataList(grpc.chord.Chord.DataList request,
                            io.grpc.stub.StreamObserver<grpc.chord.Chord.RetCode> responseObserver) {
            System.out.println("receiving putDataList()");
            for(Chord.Data item : request.getDataListList()) {
                Long id = item.getId();
                String data = item.getData();
                m_node.dataMap.put(id, data);
            }
            
            m_node.uploadStat = true;
            Chord.RetCode.Builder retBuilder = Chord.RetCode.newBuilder();
            retBuilder.setIsSuccess(true);
            responseObserver.onNext(retBuilder.build());
            responseObserver.onCompleted();
        }

    }

}
