package servidor.storage;

import api.multicast.PingReceiver;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static api.storage.Namenode.PATH;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class NamenodeServerLauncher {
    public static void main(String[] args) throws UnknownHostException {
        String URI_BASE;
        try {
            URI_BASE = args[0];
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = String.format("http://%s:%d/", InetAddress.getLocalHost().getHostAddress(),9998);
        }

        ResourceConfig config = new ResourceConfig();
        config.register(new NamenodeServer());

        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
        System.err.println("Namenode Server ready at .... "+URI_BASE);
        PingReceiver pingReceiver = new PingReceiver(URI_BASE,"Namenode");
        Thread thread = new Thread( pingReceiver);
        thread.run();
    }
}
