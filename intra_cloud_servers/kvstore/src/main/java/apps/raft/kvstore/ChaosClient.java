package apps.raft.kvstore;

//import chaosmonkey.ChaosMonkeyGrpc;
import kvstore.*;
import chaosmonkey.*;
import java.util.List;
import java.util.ArrayList;
import java.util.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.awt.*;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

public class ChaosClient {
    private final ManagedChannel[] channels;
    private final ChaosMonkeyGrpc.ChaosMonkeyBlockingStub[] stubs;
    public int numOfServers;
    private static final String configFileName = "serverConfig.txt";
    private String[] ips;
    private String[] ports;

    public ChaosClient(String[] ips, String[] ports) {
        this.numOfServers = ips.length;
        this.channels = new ManagedChannel[numOfServers];
        this.stubs = new ChaosMonkeyGrpc.ChaosMonkeyBlockingStub[numOfServers];
        this.ips = ips;
        this.ports = ports;

        for (int i = 0; i < ips.length; i++) {
            this.channels[i] = ManagedChannelBuilder.forAddress(ips[i], Integer.parseInt(ports[i])).usePlaintext(true).build();
            this.stubs[i] = ChaosMonkeyGrpc.newBlockingStub(this.channels[i]);
        }
    }


    public boolean uploadMat(List<List<Float>> matrix, int server) {
        Chaosmonkey.ConnMatrix.Builder builder = Chaosmonkey.ConnMatrix.newBuilder();
        if(matrix == null)
        	return false;
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = ChaosMonkeyGrpc.newBlockingStub(this.channels[server]);
        }
        for(int i=0; i<matrix.size(); i++){
        	Chaosmonkey.ConnMatrix.MatRow.Builder builder2 = Chaosmonkey.ConnMatrix.MatRow.newBuilder();
        	List<Float> row = matrix.get(i);
        	if(row != null)
        		builder2.addAllVals(row);
        	
        	builder.addRows(i,builder2);
        }

        Chaosmonkey.Status status = this.stubs[server].uploadMatrix(builder.build());
        return status.getRet().equals(Chaosmonkey.StatusCode.OK);
    }

    public boolean updateVal(int row, int col, float val, int server) {
        Chaosmonkey.MatValue.Builder builder = Chaosmonkey.MatValue.newBuilder();
        builder.setRow(row);
        builder.setCol(col);
        builder.setVal(val);
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = ChaosMonkeyGrpc.newBlockingStub(this.channels[server]);
        }

        Chaosmonkey.Status status= this.stubs[server].updateValue(builder.build());
        return status.getRet().equals(Chaosmonkey.StatusCode.OK);
    }

    public void shutdown() throws InterruptedException {
    		for(int i=0; i<numOfServers; i++) {
        	//channels[i].shutdown().awaitTermination(5, TimeUnit.SECONDS);
        	//stubs[i].shutdown().awaitTermination(5, TimeUnit.SECONDS);
      	}
    }

    public boolean sendChaosMonkeyCmd(String[] cmdList) {
        if(cmdList == null) return false;

        if(cmdList[0].equals("updateValue") && cmdList.length == 4) {
            int row, col = 0;
            float value = (float) 0.0;
            try {
                row = Integer.parseInt(cmdList[1]);
                col = Integer.parseInt(cmdList[2]);
                value = Float.parseFloat(cmdList[3]);
            } catch (NumberFormatException e) {
                System.out.println("updateValue command should in the format: updateValue [row] [column] [float_value]");
                return false;
            }
            for(int i=0; i<numOfServers;i++) {
                if (!updateVal(row, col, value, i))
                    return false;
            }
            return true;
        } else if (cmdList[0].equals("uploadMatrix") && cmdList.length == 2) {
            System.out.println("Here");
            int probDrop = Integer.parseInt(cmdList[1]);
            return testUploadMat(probDrop);
        }
        else {
            //System.out.println("INFO:updateValue command should in the format: updateValue [row] [column] [float_value]");
            return false;
        }
    }

    public void parseInput() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String cmd = scanner.nextLine();
            String[] splitCmd = cmd.split(" ");
            if (splitCmd[0].equals("getMatrix")) {
                getMatrix(Integer.parseInt(splitCmd[1]));
                continue;
            } else if (splitCmd[0].equals("disconnect")) {
                disconnect(Integer.parseInt(splitCmd[1]));
            }

            if(!sendChaosMonkeyCmd(splitCmd)){
                System.out.println("command failed");
            } else{
                System.out.println("Finished");
            }
        }
    }

    // disconnect this server from all other servers.
    public void disconnect(int server) {
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = ChaosMonkeyGrpc.newBlockingStub(this.channels[server]);
        }

        Chaosmonkey.ConnMatrix matrix = this.stubs[server].getMatrix(Chaosmonkey.EmptyRequest.newBuilder().build());
        int rows = this.ips.length;
        List<List<Float>> rowList = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            List<Float> rowToList = new ArrayList<>();
            Chaosmonkey.ConnMatrix.MatRow row = matrix.getRows(i);
//            float[] rowArray = new float[rows];
            for (int j = 0; j < rows; j++) {
//                rowArray[j] = row.getVals(j);
                rowToList.add(row.getVals(j));
            }
            rowList.add(rowToList);
//            System.out.println(Arrays.toString(rowArray));
        }
        int cols = rows;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                if ((i == server && j != server) || (i != server && j == server)) {
                    List<Float> row1 = rowList.get(i);
                    List<Float> row2 = rowList.get(j);
                    row1.set(j, 0.0F);
                    row2.set(i, 0.0F);
                }
            }
        }

        for (int i = 0; i < rows; i++) {
            uploadMat(rowList, i);
        }
        
    }

    /* maxFailureProb ranges from 0 exclusive to 100 inclusive */
    public boolean testUploadMat(double maxFailureProb) {
            if( maxFailureProb <= 0 || maxFailureProb > 100) {
                return false;
            }
            double divisor = 100.0 / maxFailureProb;
    		// create a random matrix
    		List<List<Float>> rowList = new ArrayList<>();
    		Random rand = new Random();
    		for(int i=0; i<this.numOfServers; i++) {
    			List<Float> row = new ArrayList<>();
    			for(int j=0; j<this.numOfServers; j++) {
    				if(i==j) 
    					row.add(new Float(0.0));
    				else
    					row.add(new Float(rand.nextFloat()/divisor));
    			}
    			rowList.add(row);
    		}

    		// now make grpc call to upload this matrix
    		for(int i=0; i<rowList.size(); i++){
    			for(int j=0; j<rowList.get(0).size();j++){
    				System.out.print(rowList.get(i).get(j)+", ");
    			}
    			System.out.println(" ");
    		}
    		for(int i=0; i<this.numOfServers; i++) {
    			boolean result = uploadMat(rowList, i);
    			if(!result) {
    				System.out.println("uploadMat failed for server "+i);
                    return false;
                }
    		}
            System.out.println("");
            return true;
    }

    public void testUpdateVal() {
    		// update on server 0
            for(int i=0; i<numOfServers; i++) {
                updateVal(1,0,(float) 0.5,i);
                // update on server 1
                updateVal(0,1,(float) 0.3,i);
            }
    		

    }

    public void getMatrix(int server) {
        if (channels[server].isTerminated() || channels[server].isShutdown()) {
            this.channels[server] = ManagedChannelBuilder.forAddress(ips[server], Integer.parseInt(ports[server])).usePlaintext(true).build();
            this.stubs[server] = ChaosMonkeyGrpc.newBlockingStub(this.channels[server]);
        }

        Chaosmonkey.ConnMatrix matrix = this.stubs[server].getMatrix(Chaosmonkey.EmptyRequest.newBuilder().build());
        int rows = this.ips.length;
        System.out.println("matrix: ");
        for (int i = 0; i < rows; i++) {
            Chaosmonkey.ConnMatrix.MatRow row = matrix.getRows(i);
            float[] rowArray = new float[rows];
            for (int j = 0; j < rows; j++) {
                rowArray[j] = row.getVals(j);
            }
            System.out.println(Arrays.toString(rowArray));
        }

    }

    public void repeatedUploadMat(int probDrop, int frequency) {
        if (frequency <= 0 || probDrop <= 0 || probDrop > 100) {
            return;
        }
        int sleepTime = 1000 / frequency;
        int count = 0;
        while(true) {
            try{
                Thread.sleep(sleepTime);
            } catch (Exception e) {

            }
            if (!testUploadMat(probDrop)){
                count++;
                System.out.println("Failed to uploadMatrix. Fail count: "+count);
            } 
        }
    }

    public static void main(String[] args) throws Exception {
    	  File file = new File(ChaosClient.configFileName);
    	  if(!file.exists()){
    	  	throw new FileNotFoundException(file.getPath());
    	  }

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
        System.out.println("finish reading config");
        ChaosClient client = new ChaosClient(ips, ports);
        if (args.length != 0 && args[0].equals("-t")) {
            try {
                System.out.println("Start ChaosMonkey UploadMat Test."+client.numOfServers);

                boolean result = client.testUploadMat(100);

                System.out.println("ChaosMonkey UploadMat Test Passed."+client.numOfServers);

                System.out.println("Start ChaosMonkey updateValue Test.");
                client.testUpdateVal();



            } finally {
                System.out.println("Finish ChaosMonkey Test, shutdown");
                //client.shutdown();
            }    
        } else if (args.length == 3 && args[0].equals("-r")) {
            // keep updating random matrix to servers with drop probility of 0-100, frequency per second
            try {
                int probDrop = Integer.parseInt(args[1]);
                if (probDrop <= 0 || probDrop > 100) {
                    System.out.println("drop probability should be an integer in the range (0, 100]");
                    System.out.println("Usage: -r [drop_probability] [frequency]");
                    return;
                }
                int frequency = Integer.parseInt(args[2]);
                if(frequency <= 0) {
                    System.out.println("frequency should be an integer in the range (0, 1000]");
                    System.out.println("Usage: -r [drop_probability] [frequency]");
                    return;
                }

                client.repeatedUploadMat(probDrop, frequency);

            } catch (Exception e) {

            }
            

        }else {
            client.parseInput();
        }
        //ChaosClient client = new ChaosClient(ips, ports);
        
    }


}
