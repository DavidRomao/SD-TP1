package sys.storage;

import api.RestRequests;
import api.storage.BlobStorage;
import api.storage.Datanode;
import org.glassfish.jersey.client.ClientConfig;
import utils.JSON;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeClient implements Datanode {

    //todo suportar falhas de rede temporarias
	private static Logger logger = Logger.getLogger(Datanode.class.toString() );

	private static final int INITIAL_SIZE = 32;
	private static final String DATANODE = "Datanode";
	
	private Map<String, byte[]> blocks = new HashMap<>(INITIAL_SIZE);
	private WebTarget target;
	private BlobStorage storage;
	private URI datanodeURI;
	public DatanodeClient(URI datanodeURI) {
		this.datanodeURI = datanodeURI;
		Client client = ClientBuilder.newClient(new ClientConfig());
		System.err.println("Connecting to datanode at " + datanodeURI );
		target = client.target(datanodeURI);
	}
	public DatanodeClient(URI datanodeURI, BlobStorage storage) {
		this.datanodeURI = datanodeURI;
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
	public void confirmDeletion(List<String> blocks) {
		RestRequests.makePost(target.path("/validate/delete").request()
				,Entity.entity( blocks, MediaType.APPLICATION_JSON) ,Response.class);

	}

	@Override
	@Deprecated
	public void mapper( List<String> blocks, String jobClass, String outputPrefix,String worker) {
		Response response = target.path("/mapper").
				queryParam("jobClass",jobClass).
				queryParam("outputPrefix", outputPrefix).
				queryParam("worker",worker).
				request().
				post(Entity.entity(JSON.encode(blocks), MediaType.APPLICATION_JSON));
		//Response path = makePost(target.path("/mapper").request()
		//				 	,Entity.entity(entity, mediaType));
		System.out.println("Mapper Status: " + response.getStatus());
	}

	public void asyncMapper(List<String> blocks, String jobClass, String outputPrefix, String worker, List<String> workers){
		
        int tries = 0;
        Future<Response> response = null;
        while (response == null && tries < 5) {
            try {
        				response = target.path("/mapper").
        				queryParam("jobClass",jobClass).
        				queryParam("outputPrefix", outputPrefix).
        				queryParam("worker",worker).
        				request().
        				async().
        				post(Entity.entity(JSON.encode(blocks), MediaType.APPLICATION_JSON),
        						new InvocationCallback<Response>() {
        							@Override
        							public void completed(Response response) {
        								System.out.println("Map task completed by " + worker);
        								workers.remove(worker);
        								System.out.println("Mapper Status: " + response.getStatus() );
        							}

        							@Override
        							public void failed(Throwable throwable) {
        								System.out.println("Map task failed by " + worker + " at " + datanodeURI);
        							}
        						});
        		//Response path = makePost(target.path("/mapper").request()
        		//				 	,Entity.entity(entity, mediaType));
        		System.out.println("Mapper Status: " + response.isDone());
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
	}

	@Override
	@Deprecated
	public void reducer(String inputPrefix, String jobClass, String outputPrefix, int outPartitionSize,int partitionCounter) {
		// TODO Auto-generated method stub
		Response response = target.path("/reducer").
				queryParam("inputPrefix",inputPrefix).
				queryParam("jobClass", jobClass).
				queryParam("outputPrefix",outputPrefix).
				queryParam("outputPartitionSize", outPartitionSize).
				queryParam("partitionCounter",partitionCounter).
				request().
				post(null);
		System.out.println("Reducer Status: " + response.getStatus());
	}

	public void asyncReducer(String inputPrefix, String jobClass, String outputPrefix, int outPartitionSize, int partitionCounter, List<String> keys) {
		// TODO Auto-generated method stub
		
        int tries = 0;
        Future<Object> response = null;
        while (response == null && tries < 5) {
            try {
        				response = target.path("/reducer").
        				queryParam("inputPrefix",inputPrefix).
        				queryParam("jobClass", jobClass).
        				queryParam("outputPrefix",outputPrefix).
        				queryParam("outputPartitionSize", outPartitionSize).
        				queryParam("partitionCounter",partitionCounter).
        				request().
        				async().
        				post(Entity.json(""), new InvocationCallback<Object>() {
        					@Override
        					public void completed(Object o) {
        						keys.remove(inputPrefix);
        					}

        					@Override
        					public void failed(Throwable throwable) {
        						System.err.println("Reduce task " + inputPrefix + " failed at " + datanodeURI);
        					}
        				});
        		System.out.println("Reducer Status: " + response.isDone());
            	
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
	}

}
