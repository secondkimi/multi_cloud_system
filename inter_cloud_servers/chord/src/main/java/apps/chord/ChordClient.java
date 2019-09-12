package apps.chord;

import java.util.Scanner;
import grpc.chord.ChordServerStoreGrpc;
import grpc.chord.StatStoreGrpc;
import io.grpc.ManagedChannel;
import java.io.FileNotFoundException; 
import java.io.FileOutputStream; 
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ChordClient {
    private ManagedChannel chordChannel;
    private ChordServerStoreGrpc.ChordServerStoreBlockingStub chordStub;
    private ManagedChannel statChannel;
    private StatStoreGrpc.StatStoreBlockingStub statStub;
    ChordNode[] chordNodes;

    public ChordClient(String configFile) {
        try{

            File file = new File(configFile);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            int i = 0;
            if((line = br.readLine()) == null) {
                br.close();
                System.out.println("Incorrect format in config file "+configFile+"\n The first line number be number of servers");
                System.exit(0);
            }
            int numOfServers = Integer.parseInt(line);
            chordNodes = new ChordNode[numOfServers];
            while((line = br.readLine()) != null && i < numOfServers) {
                String[] sp = line.split(":");
                // TODO: now set each node to be alive.
                chordNodes[i++] = new ChordNode(sp[0],Integer.parseInt(sp[1]), true);
            }
            br.close();
        } catch (Exception e) {

        }
        
    }

    public void run() {
        ChordCommand command = new ChordCommand(chordNodes);
        System.out.print("> ");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            command.execute(scanner.nextLine());
        }
    }

    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("arg 0 must be config file name");
        }
        ChordClient client = new ChordClient(args[0]);
        client.run();
    }
}
