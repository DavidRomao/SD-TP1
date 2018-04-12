package servidor.storage;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.mapreduce.Jobs;
import sys.mapreduce.JsonBlobWriter;
import sys.mapreduce.MapReducer;
import sys.storage.BlobStorageClient;
import utils.Base58;
import utils.JSON;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeServer implements Datanode {

	public static final int WaitingTime = 20 * 1000;
    private final URI uri;
    private String base_uri;
	private ConcurrentMap<String,Long> unverifiedBlocksTime = new ConcurrentHashMap<>();
	private ConcurrentMap<String,String> unverifiedBlocksBlobname = new ConcurrentHashMap<>();
	private BlobStorage storage;

    private String MapOutputBlobNameFormat;


    @SuppressWarnings("InfiniteLoopStatement")
	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
		System.err.println("URI base" + base_uri);
		// Garbage Collector
		// Launch the thread
//		garbageCollectorLauncher();
        this.uri = URI.create(base_uri);



    }

	@SuppressWarnings("InfiniteLoopStatement")
	private void garbageCollectorLauncher(){
		new Thread( () -> {
			storage = new BlobStorageClient();
			Namenode namenodeClient = storage.getNamenode();
			while (true) {
				try {
                    Thread.sleep(WaitingTime);
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
            System.out.println(e.getCause());
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
	public void mapper(String jobClassBlob, String blob, List<String> blocks, String outputPrefix) {
        MapOutputBlobNameFormat = outputPrefix + "-map-%s-" + uri.getHost()+":"+ uri.getPort();
        System.err.println("Map method invoked");
        MapReducer job = Jobs.newJobInstance(storage, jobClassBlob).instance;

        job.setYielder( (key,val) -> jsonValueWriterFor( key ).write(val));

        job.map_init();

        for (String block : blocks) {
            for (String line : new String(readBlock(block)).split("\\n")  ) {
                job.map(blob, line);
            }

        }

        job.map_end();

        writers.values().forEach( JsonBlobWriter::close );
        writers.clear();


	}


    private Map<Object, JsonBlobWriter> writers = new ConcurrentHashMap<>();

    private JsonBlobWriter jsonValueWriterFor(Object key ) {
        JsonBlobWriter res = writers.get( key );
        if( res == null ) {
            String b58key = Base58.encode( JSON.encode( key ) );
            BlobStorage.BlobWriter out = storage.blobWriter( String.format(MapOutputBlobNameFormat, b58key));
            writers.put(key,  new JsonBlobWriter(out));
        }
        return writers.get(key);
    }


    @Override
	public void reducer(String job, String inputPrefix , String outputPrefix, int outPartitionSize) {
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


