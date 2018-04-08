package servidor.storage;

import api.mapreduce.ComputeNode;
import api.storage.Datanode;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;


public class DatanodeServer implements Datanode,ComputeNode {

	private String base_uri;

	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
	}

	@Override
	public String createBlock(byte[] data){
		try {
			String id = Random.key64();
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			out.close();

			System.out.println("block created : " + base_uri +"/"+id);
			return base_uri + "/" + id;
      
		}catch(IOException e) {
			// never happens, the block is always created
			System.err.println("Internal Error!");
			return null;
		}
	}

	@Override
	public void deleteBlock(String block) {
		System.out.println("DatanodeServer.deleteBlock");
		System.out.println("block = " + block);
		File file = new File(block);
		if(file.exists()) {
			boolean delete = file.delete();
			assert delete;
		}else {
			throw new WebApplicationException( Status.NOT_FOUND);
		}
		
	}

	@Override
	public byte[] readBlock(String block) {
		try{
			System.out.println("DatanodeServer.readBlock");
			System.out.println("block = " + block);
			File file = new File(block);
			assert file.exists();
			InputStream in = new FileInputStream(file);
			byte[] blob= new byte[(int)file.length()];
			in.read(blob);
			in.close();
			return blob;
		}catch(IOException e) {
			throw new WebApplicationException( Status.NOT_FOUND );
		}
	}
	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InvalidArgumentException {
		//TODO  implement mapreduce
	}
}


