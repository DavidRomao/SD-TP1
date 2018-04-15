package api.storage;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path(Namenode.PATH)
public interface Namenode {

	static final String PATH="/namenode";

	/**
	 *
	 * @param prefix
	 * @return
	 * @apiNote 200 OK [empty List]
	 */
	@GET
	@Path("/list")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> list( @QueryParam("prefix") String prefix);
	// 200 OK [empty List]

	/**
	 *
	 * @param name
	 * @return the blocks containing the blob {@code name}
	 * @apiNote 200 OK | 404 Not Found
	 */
	@GET
	@Path("/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	List<String> read(@PathParam("name") String name);
	// 200 OK | 404 Not Found

	/**
	 * Registers a new blob {@code name} and the {@code blocks} location
	 * @param name
	 * @param blocks
	 * @apiNote 204 No Content | 409 Conflict
	 */
	@POST
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void create(@PathParam("name") String name, List<String> blocks);
	// 204 No Content | 409 Conflict

	/**
	 * Changes the locations of {@code name} blocks
	 * @param name
	 * @param blocks
	 * @apiNote 204 No Content | 404 Not Found
	 */
	@PUT
	@Path("/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	void update(@PathParam("name") String name, List<String> blocks);


	/**
	 *	All blocks with {@code prefix} are deleted
	 * @param prefix
	 * @apiNote 204 No Content | 404 Not Found
	 */
	@DELETE
	@Path("/list/")
	void delete( @QueryParam("prefix") String prefix);
	// 204 No Content | 404 Not Found

	/**
	 * Checks if a block belongs to a blob
	 * @param name
	 * @param block
	 * @return
	 */
	@GET
	@Path("/checkBlock/{name}/{block}")
	@Produces(MediaType.APPLICATION_JSON)
	boolean exists(@PathParam("name") String name, @PathParam("block") String block);

	/**
	 * Makes a deep search for a block through all the blobs
	 * @param block
	 * @return
	 */
	@GET
	@Path("/checkBlock/{block}")
	@Produces(MediaType.APPLICATION_JSON)
	boolean exists(@PathParam("block") String block);
}