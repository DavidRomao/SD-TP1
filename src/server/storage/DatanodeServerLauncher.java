package server.storage;

import api.multicast.PingReceiver;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

import static api.storage.Namenode.PATH;

/**
 * @author David Romao 49309
 */
public class DatanodeServerLauncher {

    public static void main(String[] args) {


        String URI_BASE;
        try {
            URI_BASE = args[0];
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = "http://0.0.0.0:9999/v1/";
        }
        ResourceConfig config = new ResourceConfig();
        config.register(new DatanodeServer());

        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
        System.err.println("Datanode Server ready at ...."+URI_BASE);

        PingReceiver pingReceiver = new PingReceiver(URI_BASE+PATH,"datanode");
        Thread thread = new Thread( pingReceiver);
        thread.run();
    }
}