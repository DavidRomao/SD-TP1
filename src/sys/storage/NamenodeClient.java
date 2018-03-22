package sys.storage;

import api.multicast.Multicast;
import api.storage.Namenode;
import com.google.gson.Gson;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Logger;

/*
 * Fake NamenodeClient client.
 * 
 * Rather than invoking the Namenode via REST, executes
 * operations locally, in memory.
 * 
 * Uses a trie to perform efficient prefix query operations.
 */
public class NamenodeClient implements Namenode {

    private static final String NAMENODE = "namenode";
    private static Logger logger = Logger.getLogger(NamenodeClient.class.toString() );
	private Gson gson;

//    Trie<String, List<String>> names = new PatriciaTrie<>();

	private WebTarget target;
	public NamenodeClient() {
        Multicast multicast= new Multicast();
        gson  = new Gson();
        try {
            String namenodeURI = multicast.send(NAMENODE.getBytes());
            System.err.println("Namenode server uri : " + namenodeURI);
            Client client = ClientBuilder.newClient(new ClientConfig());
            target = client.target(UriBuilder.fromUri(namenodeURI));

        } catch (UnknownHostException e) {
//            System.err.println("Could not send multicast packet to discover server address");
            logger.severe("Could not send multicast packet to discover server address");
        }
    }

	@Override
	public List<String> list(String prefix) {
	    //todo find out how query params work
        Invocation.Builder request = target.path("list/").queryParam("prefix",prefix).request(MediaType.APPLICATION_JSON);
        byte[] data = request.get(byte[].class);
        List<String> list = gson.fromJson(new String(data), List.class);
        return list;
	}

	@Override
	public void create(String name,  List<String> blocks) {
        WebTarget path = target.path(name);
        System.out.println("NamenodeClient.create");
        System.out.println(path.getUri());
        Response post = path.request(MediaType.APPLICATION_JSON).post(Entity.entity(gson.toJson(blocks), MediaType.APPLICATION_JSON));
        System.out.println("post = " + post.getStatus());
	}

	@Override
	public void delete(String prefix) {
        WebTarget path = target.path("/list").queryParam("prefix",prefix);
        Response delete = path.request().delete();
        System.out.println("delete.getStatus() = " + delete.getStatus());
    }

	@Override
	public void update(String name, List<String> blocks) {
        WebTarget path = target.path(name);
        Response put = path.request().put(Entity.entity(gson.toJson(blocks), MediaType.APPLICATION_JSON));
        System.out.println("put = " + put.getStatus());
    }

	@Override
	public List<String> read(String name) {
        WebTarget path = target.path(name);
        byte[] bytes = path.request().get(byte[].class);
        return  gson.fromJson(new String(bytes), List.class);
    }
}
