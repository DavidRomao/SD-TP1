package sys.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;

import com.google.gson.Gson;

import api.multicast.Multicast;
import api.storage.Datanode;
import utils.Random;

/*
 * Fake Datanode client.
 * 
 * Rather than invoking the Datanode via REST, executes
 * operations locally, in memory.
 * 
 */
public class DatanodeClient implements Datanode {
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private static final String DATANODE = "datanode";
	
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);
	private WebTarget target;
	
	public DatanodeClient(String datanodeURI) {
		 Client client = ClientBuilder.newClient(new ClientConfig());
		 target = client.target(UriBuilder.fromUri(datanodeURI));
	}
	
	@Override
	public String createBlock(byte[] data) {
		Response response = target.request().post(Entity.entity( data, MediaType.APPLICATION_OCTET_STREAM));
		System.out.println(response.getStatus());
		return response.readEntity(String.class);
	}

	@Override
	public void deleteBlock(String block) {
		Response response = target.path(block).request().delete();
		System.out.println(response.getStatus());
	}

	@Override
	public byte[] readBlock(String block) {
		Response response = target.path(block).request().get();
		byte[] content = response.readEntity(byte[].class);
		System.out.println(response.getStatus());
		return content;
	}
}
