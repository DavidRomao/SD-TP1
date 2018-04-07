package server.storage;

import api.multicast.PingReceiver;
import api.storage.Datanode;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

/**
 * @author David Romao 49309
 */

public class DatanodeServerLauncher {

    public static void main(String[] args) {


        String URI_BASE;
        try {
            URI_BASE = args[0];
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = "http://0.0.0.0:9999/";
        }
        ResourceConfig config = new ResourceConfig();
        DatanodeServer datanodeServer = new DatanodeServer(URI_BASE + Datanode.PATH);
        config.register(datanodeServer) ;

        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
        System.err.println("Datanode Server ready at ...."+URI_BASE);

        PingReceiver pingReceiver = new PingReceiver(URI_BASE+Datanode.PATH,"datanode");
        Thread thread = new Thread( pingReceiver);
        thread.run();
    }
}
