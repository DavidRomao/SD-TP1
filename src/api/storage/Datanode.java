package api.storage;

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
	@Path("/validate/delete")
	@Consumes(MediaType.APPLICATION_JSON)
    void confirmDeletion(List<String> blocks,@QueryParam("name") String name);

    @POST
	@Path("/mapper")
	@Consumes(MediaType.APPLICATION_JSON)
	void mapper( List<String> blocks, @QueryParam("jobClass")String jobClass, @QueryParam("blob") String blob,
				 					  @QueryParam("outputPrefix") String outputPrefix,@QueryParam("worker") String worker);


    @POST
	@Path("/reducer")
	@Produces(MediaType.APPLICATION_JSON)
	void reducer(@QueryParam("inputPrefix")String inputPrefix,
				 @QueryParam("jobClass") String jobClass,
				 @QueryParam("outputPrefix") String outputPrefix,
				 @QueryParam("outputPartitionSize") int outPartitionSize,
				 @QueryParam("partitionCounter") int partitionCounter);
}
