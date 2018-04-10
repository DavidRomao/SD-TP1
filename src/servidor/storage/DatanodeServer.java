package servidor.storage;

import api.mapreduce.ComputeNode;
import api.storage.Datanode;
import servidor.mapreduce.ComputeNodeClient;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.net.MalformedURLException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeServer implements Datanode,ComputeNode {

	public static final int WaitingTime = 20 * 1000;
	private String base_uri;
	ConcurrentMap<String,Long> unverifiedBlocks = new ConcurrentHashMap<>();
	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
		System.err.println("URI base" + base_uri);


		// Garbage Collector
		// Launch the thread
		new Thread( () -> {
			try {
				unverifiedBlocks.forEach((key, time) -> {
                    if (System.currentTimeMillis() - time > WaitingTime) {
                        boolean delete = new File(key).delete();
                        if (delete)
                            System.err.println("File deleted with success");
                    }

                });
                Thread.sleep(WaitingTime);
			} catch (InterruptedException e) {
				System.out.println("Thread Sleep interrupted");
			}
		}).start();
	}

	@Override
	public String createBlock(byte[] data){

		try {
			String id = Random.key64();
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			out.close();
			System.out.printf("block created : %s/%s\n", base_uri, id);
			unverifiedBlocks.put(id,System.currentTimeMillis());
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
			throw new WebApplicationException( Status . NOT_FOUND );
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
	public void confirmBlocks(List<String> blocks) {
		// remove blocks from the unverified blocks list
		blocks.forEach( block -> unverifiedBlocks.remove(block) );
	}

	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InvalidArgumentException {
		//TODO  implement mapreduce
		try {
			ComputeNodeClient mapReducer= new ComputeNodeClient();
			mapReducer.mapReduce(jobClassBlob, inputPrefix, outputPrefix, outPartSize);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}


