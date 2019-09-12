package apps.chord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChordRunner {

    public static void main(String[] args) {
        String conifg_file = "serverConfig.conf";
        BufferedReader reader;
        ExecutorService executor;
        executor = Executors.newFixedThreadPool(1024);


        try {
            reader = new BufferedReader(new FileReader(conifg_file));
            int num_of_servers = Integer.parseInt(reader.readLine());
            ChordServer[] servers = new ChordServer[num_of_servers];
            for (int i = 0; i < num_of_servers; i++) {
                String serverLine = reader.readLine();
                String[] argv = serverLine.split(":");
                String ip = argv[0];
                int port = Integer.parseInt(argv[1]);
                servers[i] = new ChordServer(ip, port);
            }
            ServerCreate task_create = new ServerCreate(servers[0]);
            executor.submit(task_create);
            Thread.sleep(8000);
            for (int i = 1; i < num_of_servers; i++) {
                if (i < 5) {
                    Thread.sleep(5000);
                } 
                else if( i % 5 == 0) {
                    Thread.sleep(4000);
                }
                ChordNode prev_created_node = null;
                if(i >= 10) {
                    prev_created_node = new ChordNode(servers[i%5].m_node.get_ip(), servers[i%5].m_node.get_m_port());
                } else if(i>=5) {
                    prev_created_node = new ChordNode(servers[i%2].m_node.get_ip(), servers[i%2].m_node.get_m_port());
                } else {
                    prev_created_node = new ChordNode(servers[0].m_node.get_ip(), servers[0].m_node.get_m_port());
                }
                ServerRun task_run = new ServerRun(servers[i], prev_created_node);
                executor.submit(task_run);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}


class ServerCreate implements Callable<ChordServer> {
    private final ChordServer server;

    public ServerCreate(ChordServer server) {
        this.server = server;
    }

    @Override
    public ChordServer call() throws Exception{
        try {
            server.start(10);
            server.m_node.create();
            server.blockUntilShutdown();
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }
}

class ServerRun implements Callable<ChordServer> {
    private final ChordServer server;
    private final ChordNode toJoinNode;

    public ServerRun(ChordServer server, ChordNode toJoinNode) {
        this.server = server;
        this.toJoinNode = toJoinNode;
    }

    @Override
    public ChordServer call() throws Exception {
        try {
            server.start(10);
            server.m_node.join(toJoinNode);
            server.blockUntilShutdown();
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }
}
