package servidor.storage;

import api.storage.Datanode;
import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Lists;
import sys.mapreduce.Jobs;
import sys.mapreduce.JsonBlobWriter;
import sys.mapreduce.MapReducer;
import sys.mapreduce.MapperTask;
import sys.mapreduce.ReducerTask;
import sys.storage.BlobStorageClient;
import utils.Base58;
import utils.JSON;
import utils.Random;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.google.gson.Gson;

import java.io.*;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeServer implements Datanode {

	public static final int WaitingTime = 20 * 1000;
	private String base_uri;
	ConcurrentMap<String,Long> unverifiedBlocks = new ConcurrentHashMap<>();
	private List results;
	
	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
		System.err.println("URI base" + base_uri);


		// Garbage Collector
		// Launch the thread
//		new Thread( () -> {
//			try {
//				unverifiedBlocks.forEach((key, time) -> {
//                    if (System.currentTimeMillis() - time > WaitingTime) {
//                        boolean delete = new File(key).delete();
//                        if (delete)
//                            System.err.println("File deleted with success");
//                    }
//
//                });
//                Thread.sleep(WaitingTime);
//			} catch (InterruptedException e) {
//				System.out.println("Thread Sleep interrupted");
//			}
//		}).start();
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
	
	@SuppressWarnings("unchecked")
	@Override
	public void mapper(MapReducer job, String block, String outputPrefix) {
		
		
		//new MapperTask("Datanode", storage, jobClassBlob, inputPrefix, outputPrefix).execute();
		List results = new LinkedList<LinkedHashMap<Object, Object>>();
		job.setYielder( (key,val) -> results.add(new LinkedHashMap<>().put(key, val)));
		
		job.map_init();
		String[] lines = new String(readBlock(block)).split("\n");
		for(int i=0;lines[i]!= null;i++) {
			System.out.println(lines[i]);
			job.map( block, lines[i] );
		}

		job.map_end();
		
	}
	
	@Override
	public void reducer(MapReducer job, String inputPrefix , String outputPrefix, int outPartitionSize) {
		/*
		Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
				.map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
				.collect( Collectors.toSet() );
			
			
			AtomicInteger partitionCounter = new AtomicInteger(0);
			Lists.partition( new ArrayList<>( reduceKeyPrefixes), outPartitionSize).forEach(partitionKeyList -> {
					
					String partitionOutputBlob = String.format("%s-part%04d", outputPrefix, partitionCounter.incrementAndGet());
					
					BlobWriter writer = storage.blobWriter(partitionOutputBlob);

					partitionKeyList.forEach( keyPrefix -> {
						//new ReducerTask("client", storage, jobClassBlob, keyPrefix, outputPrefix).execute(writer);			
					});			
					
					writer.close();
				});
		*/
	}
}


