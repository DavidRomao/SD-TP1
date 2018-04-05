package server.storage;

import api.multicast.PingReceiver;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;

import static api.storage.Namenode.PATH;

/**
 * @author David Romao 49309
 */
public class NamenodeServerLauncher {
    public static void main(String[] args) {
        String URI_BASE;
        try {
            URI_BASE = args[0];
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = "http://localhost:7777/v1";
        }

        ResourceConfig config = new ResourceConfig();
        config.register(new NamenodeServer());

        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
        System.err.println("Namenode Server ready at .... "+URI_BASE);
        PingReceiver pingReceiver = new PingReceiver(URI_BASE+PATH,"namenode");
        Thread thread = new Thread( pingReceiver);
        thread.run();
    }
}
