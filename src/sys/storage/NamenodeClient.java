package sys.storage;

import api.RestRequests;
import api.multicast.Multicast;
import api.storage.Namenode;
import com.google.gson.Gson;
import org.glassfish.jersey.client.ClientConfig;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Namenode via REST
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class NamenodeClient implements Namenode {

    private static final String NAMENODE = "Namenode";
	private Gson gson;

//    Trie<String, List<String>> names = new PatriciaTrie<>();

	private WebTarget target;
	public NamenodeClient() {
        Multicast multicast = new Multicast();
        gson = new Gson();
        try {
            String namenodeURI = null;
            while (namenodeURI == null) {
                //todo change timeout
                Set<String> send = multicast.send(NAMENODE.getBytes(), 100);
                if (send.size()> 0)
                    namenodeURI = send.iterator().next();
            }
            System.err.println("Namenode discovered at " + namenodeURI);
            Client client = ClientBuilder.newClient(new ClientConfig());
            target = client.target(UriBuilder.fromUri(namenodeURI + "namenode"));
        }catch (NoSuchElementException e){
            System.err.println("No namenodes available");
//            System.exit(0);
        }
    }

	@SuppressWarnings("unchecked")
    @Override
	public List<String> list(String prefix) {
	    //todo find out how query params work
        Invocation.Builder request = target.path("list").queryParam("prefix",prefix).request(MediaType.APPLICATION_JSON);
        byte[] data = RestRequests.makeGet(request, byte[].class);
        List<String> list = gson.fromJson(new String(data), List.class);
        System.err.printf("Received list with %d blobs for prefix |%s|\n", list.size(), prefix);
        return list;
	}

	@Override
	public void create(String name,  List<String> blocks) {

        WebTarget path = target.path(name);

        System.err.println("NamenodeClient.create");

        System.err.println(path.getUri());

//        Response post = path.request(MediaType.APPLICATION_JSON).post(Entity.entity(gson.toJson(blocks), MediaType.APPLICATION_JSON));
        Response post = RestRequests.makePost(path.request(MediaType.APPLICATION_JSON), Entity.entity( gson.toJson(blocks), MediaType.APPLICATION_JSON));
        System.err.println("post = " + post.getStatus());


	}

	@Override
	public void delete(String prefix) {
        WebTarget path = target.path("/list").queryParam("prefix",prefix);
        RestRequests.makeDelete(path.request());
    }


    @Override
	public void update(String name, List<String> blocks ) {
        WebTarget path = target.path(name);
        Response response = RestRequests.makePut(path.request(), Entity.json( gson.toJson( blocks) ));
        System.out.println("NamenodeClient.update");
        System.out.println("response = " + response.getStatus());
        if (response.getStatus()>= 400)
            response.getStringHeaders().forEach( ( a,b) -> System.out.printf("%s : %s ",a,b));
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
