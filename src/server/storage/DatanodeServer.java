package server.storage;

import api.storage.Datanode;
import utils.Base58;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;


public class DatanodeServer implements Datanode {
	
	@Override
	public String createBlock(byte[] data){
		try {
			String id = Base58.encode(data);
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			out.close();
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
			in.read(blob);
			in.close();
			return blob;
		}catch(IOException e) {
			throw new WebApplicationException( Status.NOT_FOUND );
		}

	}


	
}


