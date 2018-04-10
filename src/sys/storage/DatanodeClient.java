package sys.storage;

import api.storage.Datanode;
import org.glassfish.jersey.client.ClientConfig;

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
	
	public DatanodeClient(URI datanodeURI) {
		 Client client = ClientBuilder.newClient(new ClientConfig());
		 target = client.target(datanodeURI);
	}

	/**
	 *
	 * @param data the block content
	 * @return the complete block uri
	 */
	@Override
	public String createBlock(byte[] data) {
		return makePost(target.request(), Entity.entity( data, MediaType.APPLICATION_OCTET_STREAM) ,String.class);
	}

	/**
	 *
	 * @param block the block id, not the uri
	 */
	@Override
	public void deleteBlock(String block) {
		Response response = makeDelete(target.path(block).request());
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
		return  makeGet( target.path(block).request(),byte[].class);
	}

    public static  <T> T makeGet(Invocation.Builder request, Class<T> aClass) {
        int tries = 0;
        T response = null;
        while (response == null && tries < 5) {
            try {
                response = request.get(aClass);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }



    public static  <D,T> D makePost(Invocation.Builder request, Entity<T> entity,Class<D> aClass) {
        int tries = 0;
        D response = null;
        while (response == null && tries < 5) {
            try {
                response = request.post(entity,aClass);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

    public static Response makeDelete(Invocation.Builder request){
        int tries = 0;
        Response response = null;
        while (response == null && tries < 5) {
            try {
                response = request.delete();
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

	@Override
	public void confirmBlocks(List<String> blocks) {
        Response response = makePost(target.path("/validate").request()
                            ,Entity.entity(blocks, MediaType.APPLICATION_JSON)
                            ,Response.class);
        //todo
	}

}
