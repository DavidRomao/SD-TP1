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
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("/validate")
	@POST

	/*
	void confirmBlocks(List<String> blocks);
	@POST
	//@Path()
	@Produces()
	void mapper(@QueryParam("jobClassBlob") String jobClassBlob, @QueryParam("inputPrefix") String inputPrefix , @QueryParam("outputPrefix") String outputPrefix);
	
	@POST //Ou get?
	//@Path()
	void reducer( @QueryParam("jobClassBlob") String jobClassBlob, @QueryParam("inputPrefix") String inputPrefix , @QueryParam("outputPrefix") String outputPrefix);
}
