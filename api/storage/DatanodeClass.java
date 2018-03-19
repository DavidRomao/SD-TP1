package api.storage;

import java.io.*;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import utils.Base58;

public class DatanodeClass implements Datanode{
	
	@Override
	public String createBlock(byte[] data){
		try {
			String id = Base58.encode(data);
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			return id;
		}catch(IOException e) {
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
			in.read(blob, 0, (int)file.length());
			return blob;
		}catch(IOException e) {
			throw new WebApplicationException( Status.NOT_FOUND );
		}

	}
	
	
}
