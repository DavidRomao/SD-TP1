package servidor.storage;

import api.multicast.PingReceiver;
import api.storage.Datanode;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */

public class DatanodeServerLauncher {
    public static void main(String[] args) throws UnknownHostException {

        String URI_BASE;


        try {
            URI_BASE = args[0];
            System.err.println("Created new datanode server with new URI");
            System.err.println("args.length = " + args.length);
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = String.format("http://%s:%d/",InetAddress.getLocalHost().getHostAddress(),9999);
            System.err.println("Created new datanode server with base URI");
            System.err.println("args.length = " + args.length);
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
