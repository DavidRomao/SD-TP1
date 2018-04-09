package api.storage;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path( Datanode.PATH ) 
public interface Datanode {

	static final String PATH = "datanode";
	
	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	String createBlock(byte[] data);
	
	@DELETE
	@Path("/{block}")
	void deleteBlock(@PathParam("block") String block);
	
	@GET
	@Path("/{block}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	byte[] readBlock(@PathParam("block") String block);

	@POST
	@Path("/validate")
	@Consumes(MediaType.APPLICATION_JSON)
	void confirmBlocks(List<String> blocks);
	/*
	@POST
	@Path("/")
	@Produces()
	@Consumes()
	void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize);
	*/
}
