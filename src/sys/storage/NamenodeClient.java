package sys.storage;

import api.RestRequests;
import api.multicast.Multicast;
import api.storage.Namenode;
import com.google.gson.Gson;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.List;
import java.util.NoSuchElementException;

/*
 *
 * Namenode via REST
 */
@SuppressWarnings("deprecation")
public class NamenodeClient implements Namenode {

    private static final String NAMENODE = "Namenode";
	private Gson gson;

//    Trie<String, List<String>> names = new PatriciaTrie<>();

	private WebTarget target;
	public NamenodeClient() {
        Multicast multicast = new Multicast();
        gson = new Gson();
        try {
            String namenodeURI = multicast.send(NAMENODE.getBytes(), 1000).iterator().next();
            Client client = ClientBuilder.newClient(new ClientConfig());
            target = client.target(UriBuilder.fromUri(namenodeURI + "namenode"));
        }catch (NoSuchElementException e){
            System.err.println("No namenodes available");
            System.exit(0);
        }
    }

	@SuppressWarnings("unchecked")
    @Override
	public List<String> list(String prefix) {
	    //todo find out how query params work
        Invocation.Builder request = target.path("list/").queryParam("prefix",prefix).request(MediaType.APPLICATION_JSON);
        byte[] data = RestRequests.makeGet(request, byte[].class);
        List<String> list = gson.fromJson(new String(data), List.class);
        System.err.printf("Received list with %d blobs for prefix |%s|\n", list.size(), prefix);
        return list;
	}

	@Override
	public void create(String name,  List<String> blocks) {
        Invocation.Builder request = target.request(MediaType.APPLICATION_JSON);
        Response post = RestRequests.makePost(request, Entity.entity( gson.toJson(blocks), MediaType.APPLICATION_JSON));
	}

	@Override
	public void delete(String prefix) {
        WebTarget path = target.path("/list").queryParam("prefix",prefix);
        RestRequests.makeDelete(path.request());
    }


    @Override
	public void update(String name, List<String> blocks) {
        WebTarget path = target.path(name);
        RestRequests.makePut(path.request(),Entity.entity(gson.toJson(blocks), MediaType.APPLICATION_JSON));
    }

    @SuppressWarnings("unchecked")
    @Override
	public List<String> read(String name) {
        byte[] bytes = RestRequests.makeGet( target.path(name).request(), ( byte[].class) );
        return gson.fromJson( new String(bytes), List.class);
    }


    @Override
    public boolean exists(String name, String block) {
        WebTarget path = target.path(String.format("/checkBlock/%s/%s", name, block));
        return RestRequests.makeGet(path.request(), Boolean.class);
    }

    @Override
    public boolean exists(String block) {
        WebTarget path = target.path(String.format("/checkBlock/%s", block));
        return RestRequests.makeGet(path.request(), Boolean.class);
    }


}
