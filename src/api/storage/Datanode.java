package api.storage;

import sys.mapreduce.MapReducer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path( Datanode.PATH ) 
public interface Datanode {

	static final String PATH = "datanode";
	String BLOB_NAME = "blobName";

	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@QueryParam("blobName")
	String createBlock(byte[] data, @QueryParam(BLOB_NAME)String blobName );
	
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
	
	@POST
	@Path("/mapper")
	@Consumes(MediaType.APPLICATION_JSON)
	void mapper(MapReducer job, @QueryParam("inputPrefix") String inputPrefix, @QueryParam("outputPrefix") String outputPrefix);
	
	@POST
	@Path("/reducer")
	@Consumes(MediaType.APPLICATION_JSON)
	void reducer(MapReducer job, @QueryParam("inputPrefix") String inputPrefix, @QueryParam("outputPrefix") String outputPrefix, @QueryParam("outPartitionSize") int outPartitionSize);
}
