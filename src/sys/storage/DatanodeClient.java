package sys.storage;

import api.RestRequests;
import api.storage.BlobStorage;
import api.storage.Datanode;
import org.glassfish.jersey.client.ClientConfig;

import com.google.gson.Gson;

import utils.JSON;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/*
 * Fake Datanode client.
 * 
 * Rather than invoking the Datanode via REST, executes
 * operations locally, in memory.
 * 
 */
public class DatanodeClient implements Datanode {

    //todo suportar falhas de rede temporarias
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private static final String DATANODE = "Datanode";
	
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);
	private WebTarget target;
	private BlobStorage storage;

	public DatanodeClient(URI datanodeURI) {
		Client client = ClientBuilder.newClient(new ClientConfig());
		System.err.println("Connecting to datanode at " + datanodeURI );
		target = client.target(datanodeURI);
	}
	public DatanodeClient(URI datanodeURI, BlobStorage storage) {
		this.storage = storage;
		Client client = ClientBuilder.newClient(new ClientConfig());
		target = client.target(datanodeURI);

	}

	/**
	 *
	 * @param data the block content
	 * @return the complete block uri
	 */
	@Override
	public String createBlock(byte[] data,String blobName) {
		return RestRequests.makePost(target.queryParam(BLOB_NAME,blobName).request(), Entity.entity( data, MediaType.APPLICATION_OCTET_STREAM) ,String.class);
	}

	/**
	 *
	 * @param block the block id, not the uri
	 */
	@Override
	public void deleteBlock(String block) {
		Response response = RestRequests.makeDelete(target.path(block).request());
//		System.out.println("DatanodeClient.deleteBlock");
//		System.out.println(response.getStatus());
	}

	/**
	 *
	 * @param block the block id, not the uri
	 * @return
	 */
	@Override
	public byte[] readBlock(String block) {
		return  RestRequests.makeGet( target.path(block).request(),byte[].class);
	}


    @Override
	public void confirmBlocks(List<String> blocks) {
        Response response = RestRequests.makePost(target.path("/validate").request()
                            ,Entity.entity( blocks, MediaType.APPLICATION_JSON) ,Response.class);
	}
    
	@Override
	public void mapper(String jobClass, String inputPrefix, String outputPrefix) {
		Response response = target.path("/mapper").
				queryParam("jobClass",jobClass).
				queryParam("inputPrefix",inputPrefix).
				queryParam("outputPrefix", outputPrefix).
				request().
				post(null);
		//Response path = makePost(target.path("/mapper").request()
		//				 	,Entity.entity(entity, mediaType));
		System.out.println("Mapper Status: " + response.getStatus());
	}

	@Override
	public void reducer(String jobClass, String outputPrefix, int outPartitionSize) {
		// TODO Auto-generated method stub
		Response response = target.path("/reducer").
				queryParam("jobClass", jobClass).
				queryParam("outputPrefix",outputPrefix).
				queryParam("outputPartitionSize", outPartitionSize).
				request().
				post(null);
		System.out.println("Reducer Status: " + response.getStatus());
	}
	
}
