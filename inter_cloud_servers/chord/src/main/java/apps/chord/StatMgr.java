package apps.chord;

import grpc.chord.Stat;
import grpc.chord.StatStoreGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.lang.Thread;
import java.util.concurrent.*;
import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatMgr {
    private Map<Long, Integer> nodeStatMap; // id -> numOfKeys
    private Map<Integer, Integer> pathLengthMap; // pathLength -> count of pathLength
    private Server server;
    public static final String IP = "127.0.0.1";
    public static final int PORT = 8888;
    private static final int NUM_THREADS = 10;

    public StatMgr() {
        nodeStatMap = new HashMap<>();
        pathLengthMap = new HashMap<>();
    }

    public static void main(String[] args) {
    	  StatMgr manager = new StatMgr();
    	  try {
    	  	manager.start(NUM_THREADS);
        	manager.blockUntilShutdown();
    	  } catch (Exception e) {

    	  }
        
    }

    public void start(int numThreads) throws IOException {
        this.server = ServerBuilder.forPort(PORT)
                .addService(new StatStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();

        System.out.println("Stat manager starts at port: " + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                StatMgr.this.stop();
            }
        });
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    class StatStoreImpl extends StatStoreGrpc.StatStoreImplBase {

        @Override
        public void notifyAddKey(grpc.chord.Stat.AddKeyInfo request,
                             io.grpc.stub.StreamObserver<grpc.chord.Stat.SimpleResponse> responseObserver) {
            System.out.println("received addKeyInfo request");
            System.out.println("pathLength = " + request.getPathLength());
            int pathLength = request.getPathLength();
            pathLengthMap.put(pathLength, pathLengthMap.getOrDefault(pathLength, 0)+1);
            Stat.SimpleResponse.Builder simpleResponseBuilder =  Stat.SimpleResponse.newBuilder();
            simpleResponseBuilder.setRet(Stat.ReturnCode.SUCCESS);
            responseObserver.onNext(simpleResponseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void notifyAddListOfKeys(grpc.chord.Stat.PathLengthList request,
                             io.grpc.stub.StreamObserver<grpc.chord.Stat.SimpleResponse> responseObserver) {
            System.out.println("received NotifyAddListOfKeys request");
            
            for(Stat.PathLengthInfo pathLengthInfo : request.getPathLengthInfoList()) {
            	int pathLength = pathLengthInfo.getPathLength();
            	int count = pathLengthInfo.getPathCount();
            	pathLengthMap.put(pathLength, pathLengthMap.getOrDefault(pathLength, 0)+count);
            }
            
            Stat.SimpleResponse.Builder simpleResponseBuilder =  Stat.SimpleResponse.newBuilder();
            simpleResponseBuilder.setRet(Stat.ReturnCode.SUCCESS);
            responseObserver.onNext(simpleResponseBuilder.build());
            responseObserver.onCompleted();
        }


        @Override
        public void updateNodeInfo(grpc.chord.Stat.NodeStatInfo request,
                                   io.grpc.stub.StreamObserver<grpc.chord.Stat.SimpleResponse> responseObserver) {
            nodeStatMap.put(request.getId(), request.getNumOfKeys());
            Stat.SimpleResponse.Builder simpleResponseBuilder =  Stat.SimpleResponse.newBuilder();
            simpleResponseBuilder.setRet(Stat.ReturnCode.SUCCESS);
            responseObserver.onNext(simpleResponseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getStatics(grpc.chord.Stat.Ping1 request,
                               io.grpc.stub.StreamObserver<grpc.chord.Stat.StaticsResult> responseObserver) {
            Stat.StaticsResult.Builder resBuilder = Stat.StaticsResult.newBuilder();
            Stat.NodeStatInfo.Builder nodeStatInfoBuilder = Stat.NodeStatInfo.newBuilder();
            Stat.PathLengthInfo.Builder pathLengthInfoBuilder = Stat.PathLengthInfo.newBuilder();

            for (Long id : nodeStatMap.keySet()) {
                nodeStatInfoBuilder.setId(id);
                nodeStatInfoBuilder.setNumOfKeys(nodeStatMap.get(id));
                resBuilder.addNodeInfos(nodeStatInfoBuilder.build());
            }

            for (int pathLength : pathLengthMap.keySet()) {
                pathLengthInfoBuilder.setPathLength(pathLength);
                pathLengthInfoBuilder.setPathCount(pathLengthMap.get(pathLength));
                resBuilder.addLengthInfos(pathLengthInfoBuilder.build());
            }

            responseObserver.onNext(resBuilder.build());
            responseObserver.onCompleted();


        }


    }
}
