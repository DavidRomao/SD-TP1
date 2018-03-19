package api.storage;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path(Namenode.PATH)
public interface Namenode {

	static final String PATH="/namenode";

	@GET
	@Path("/list/")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> list( @QueryParam("prefix") String prefix);
	// 200 OK [empty List]

	@GET
	@Path("/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> read(@PathParam("name") String name);
	// 200 OK | 404 Not Found

	@POST
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void create(@PathParam("name") String name, List<String> blocks);
	// 204 No Content | 409 Conflict

	@PUT
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void update(@PathParam("name") String name, List<String> blocks);
	// 204 No Content | 404 Not Found

	@DELETE
	@Path("/list/")
	void delete( @QueryParam("prefix") String prefix);
	// 204 No Content | 404 Not Found
}