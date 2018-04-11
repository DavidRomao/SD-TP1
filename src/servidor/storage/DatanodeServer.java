package servidor.storage;

import api.storage.Datanode;
import sys.mapreduce.MapReducer;
import sys.storage.NamenodeClient;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeServer implements Datanode {

	public static final int WaitingTime = 20 * 1000;
	private String base_uri;
	private ConcurrentMap<String,Long> unverifiedBlocksTime = new ConcurrentHashMap<>();
	private ConcurrentMap<String,String> unverifiedBlocksBlobname = new ConcurrentHashMap<>();

	@SuppressWarnings("InfiniteLoopStatement")
	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
		System.err.println("URI base" + base_uri);

		// Garbage Collector
		// Launch the thread
		garbageCollectorLauncher();
	}

	@SuppressWarnings("InfiniteLoopStatement")
	private void garbageCollectorLauncher(){
		new Thread( () -> {
			NamenodeClient namenodeClient = new NamenodeClient();
			while (true) {
				try {
					unverifiedBlocksTime.forEach((key, time) -> {
						// if the block is old enough
						if (System.currentTimeMillis() - time > WaitingTime) {
							String name = unverifiedBlocksBlobname.get(key);
							// if when the block was created a blob name was sent too
							if (name != null) {
								// if it's a lost block
								if (!namenodeClient.exists(name, key)) {
									unverifiedBlocksBlobname.remove(key);
									unverifiedBlocksTime.remove(key);
									new File(key).delete();
								}
							} else // delete the block
							{
								// if a blob name was not provided when the block was created ,
								// perform a deep search on the namenode to try and find the block
								if (!namenodeClient.exists(key))
									new File(key).delete();
							}
						}

					});
					Thread.sleep(WaitingTime);
				} catch (InterruptedException e) {
					System.out.println("Thread Sleep interrupted");
				}
			}
		}).start();
	}
	@Override
	public String createBlock(byte[] data, String blobName){

		try {
			String id = Random.key64();
			File blob = new File(id);
			OutputStream out = new FileOutputStream(blob);
			out.write(data);
			out.close();
			System.out.printf("block created : %s/%s\n", base_uri, id);
			unverifiedBlocksTime.put(id,System.currentTimeMillis());
			if (blobName != null) {
				unverifiedBlocksBlobname.put(id,blobName);
			}
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
		blocks.forEach( block -> unverifiedBlocksTime.remove(block) );
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


