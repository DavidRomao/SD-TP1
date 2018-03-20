package server.storage;

import api.multicast.Multicast;
import api.storage.Namenode;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author David Romao 49309
 */
public class NamenodeServer implements Namenode {
    public static final String NAMENODE = "namenode";
    public static final URI multicastListener = URI.create("http://225.100.100.100:8080");
    private Map<String,List<String>> nametable;

    @Override
    public List<String> list(String prefix) {
        List<String> names = new LinkedList<>();
        nametable.keySet().forEach(
                s->{
                    if(s.startsWith(prefix)) {
                        names.add(s);
                }
        });
        return names;
    }

    @Override
    public List<String> read(String name) {
        List<String> strings = nametable.get(name);
        if (strings== null)
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        else
            return strings;
    }

    @Override
    public void create(String name, List<String> blocks) {
        if (blocks.size()==0)
            throw new WebApplicationException(Response.Status.NO_CONTENT);
        else if (nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else
            nametable.put(name,blocks);
    }

    @Override
    public void update(String name, List<String> blocks) {
        if (!nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else {
            List<String> put = nametable.put(name, blocks);
            throw new WebApplicationException(Response.Status.NO_CONTENT);
            //TODO  ask teacher if 200 OK is never returned
        }
    }

    @Override
    public void delete(String prefix) {
        nametable.keySet().forEach(s->{
            if (s.startsWith(prefix))
                nametable.remove(s);
        });
    }

    public static void main(String[] args) {


        String URI_BASE;
        try {
            URI_BASE = args[0];
        }catch ( ArrayIndexOutOfBoundsException e){
            URI_BASE = "http://0.0.0.0:9998/v1/";
        }

        ResourceConfig config = new ResourceConfig();
        config.register(new NamenodeServer());

        JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
        System.err.println("Server ready at ...."+URI_BASE);
        PingReceiver pingReceiver = new PingReceiver(multicastListener,URI_BASE);
        Thread thread = new Thread( pingReceiver);
        thread.run();
    }
    private static class PingReceiver implements Runnable{
        private final Multicast multicast;
        private String answer;

        public PingReceiver(URI uri,String answer) {

            this.multicast = new Multicast(uri);
            this.answer = answer;
        }

        @Override
        public void run() {
            while (true){
                try {
                    multicast.receive(NAMENODE,answer);
                } catch (UnknownHostException e) {
                    System.err.println("Multicast ip not found.");
                }
            }

        }
    }
}
