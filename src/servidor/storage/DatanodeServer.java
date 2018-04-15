package servidor.storage;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.mapreduce.Jobs;
import sys.mapreduce.JsonBlobWriter;
import sys.mapreduce.MapReducer;
import sys.mapreduce.ReducerTask;
import sys.storage.BlobStorageClient;
import sys.storage.NamenodeClient;
import utils.Base58;
import utils.JSON;
import utils.Random;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class DatanodeServer implements Datanode {

	public static final int WaitingTime = 20 * 1000;
    private final URI uri;
    private String base_uri;
	private ConcurrentMap<String,DataSet> unverifiedBlocks = new ConcurrentHashMap<>();
	private BlobStorage storage;
    private String MapOutputBlobNameFormat;

	class DataSet{
		long time;
		String name;

		public DataSet(long time, String name) {
			this.time = time;
			this.name = name;
		}
	}
    @SuppressWarnings("InfiniteLoopStatement")
	public DatanodeServer(String base_uri) {
		this.base_uri = base_uri;
		System.err.println("URI base" + base_uri);
		// Garbage Collector
		// Launch the thread
		// TODO: 14/04/18 fix and activate garbage collector
		GarbageCollectorDatanode garbageCollector = new GarbageCollectorDatanode();
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(garbageCollector, 20, 20, TimeUnit.SECONDS);
//		new Thread(new GarbageCollectorDatanode()).start();
        this.uri = URI.create(base_uri);
    }

	@SuppressWarnings("InfiniteLoopStatement")
	private class GarbageCollectorDatanode implements Runnable {
		@Override
		public void run() {
			Namenode namenodeClient = new NamenodeClient();
			while (true) {
					unverifiedBlocks.forEach((key, data) -> {
						// if the block is old enough
						if (System.currentTimeMillis() - data.time > WaitingTime) {
							String name = data.name;
							// if when the block was created a blob name was sent too
							if (name != null) {
								// if it's a lost block
								if (!namenodeClient.exists(name, key)) {
									unverifiedBlocks.remove(key);
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
			}
		}
	}

	private  void put(ConcurrentMap<String, DataSet> unverifiedBlocks, String id, long l,String name){
		unverifiedBlocks.put(id, new DataSet(l,name));
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
			if (blobName != null) {
				put(unverifiedBlocks,id,System.currentTimeMillis(),blobName );
			}else
				put(unverifiedBlocks,id,System.currentTimeMillis(),null);
			
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
		System.out.println("Received Confirmations");
		// remove blocks from the unverified blocks list
		blocks.forEach( block -> unverifiedBlocks.remove(block) );
	}

	@Override
	public void confirmDeletion(List<String> blocks, String name) {
		for (String block : blocks) {
			File file = new File(block);
			if (file.exists())
				file.delete();
		}
	}

    @SuppressWarnings("unchecked")
	@Override
	public void mapper(List<String> blocks,String jobClassBlob,  String outputPrefix,String worker) {
        MapOutputBlobNameFormat = outputPrefix + "-map-%s-" + worker;
        System.err.println("Map method invoked");
        if(storage == null) {
        	storage = new BlobStorageClient();
        }
		MapReducer job = Jobs.newJobInstance(storage, jobClassBlob).instance;

        job.setYielder( (key,val) -> jsonValueWriterFor( key ).write(val));

        job.map_init();

        for (String block : blocks) {
        	block = block.split("datanode/")[1];
            for (String line : new String(readBlock(block)).split("\\n")  ) {
                job.map(block, line);
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
	public void reducer(String inputPrefix,String jobClassBlob, String outputPrefix, int outPartitionSize,int partitionCounter) {
    	System.out.println(outPartitionSize);
    	if(storage == null) {
    		storage = new BlobStorageClient();
    	}
		String partitionOutputBlob = String.format("%s-part%04d", outputPrefix,partitionCounter);

		String reduceKey = inputPrefix.substring( inputPrefix.lastIndexOf('-')+1);

		BlobStorage.BlobWriter writer = storage.blobWriter(partitionOutputBlob);

    	new ReducerTask("reducer",storage,jobClassBlob,inputPrefix,outputPrefix)
				.execute(writer);

	}
}


