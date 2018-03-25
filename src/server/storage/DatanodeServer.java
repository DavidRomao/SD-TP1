package server.storage;

import api.storage.Datanode;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import utils.Base58;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.net.URI;


public class DatanodeServer implements Datanode {
	
	@Override
	public String createBlock(byte[] data){
		try {
			String id = Base58.encode(data);
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			return id;
		}catch(IOException e) {
			// never happens, the block is always created
			System.err.println("Internal Error!");
			return null;
		}

	}

	@Override
	public void deleteBlock(String block) {
		File file = new File(block);
		if(file.exists()) {
			file.delete();
		}else {
			throw new WebApplicationException( Status.NOT_FOUND);
		}
		
	}

	@Override
	public byte[] readBlock(String block) {
		try{
			File file = new File(block);
			InputStream in = new FileInputStream(file);
			byte[] blob= new byte[(int)file.length()];
			in.read(blob, 0, (int) file.length());
			return blob;
		}catch(IOException e) {
			throw new WebApplicationException( Status.NOT_FOUND );
		}

	}

	public static void main(String[] args) {


		String URI_BASE;
		try {
			URI_BASE = args[0];
		}catch ( ArrayIndexOutOfBoundsException e){
			URI_BASE = "http://0.0.0.0:9999/v1/";
		}

		ResourceConfig config = new ResourceConfig();
		config.register(new DatanodeServer());

		JdkHttpServerFactory.createHttpServer(URI.create(URI_BASE), config);
		System.err.println("Server ready at ...."+URI_BASE);
	}
	
	
}
