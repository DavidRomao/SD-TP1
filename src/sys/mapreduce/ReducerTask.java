package sys.mapreduce;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import jersey.repackaged.com.google.common.collect.Iterators;
import sys.mapreduce.Jobs.JobInstance;
import utils.Base58;
import utils.JSON;

import java.util.Iterator;
import java.util.stream.Collectors;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ReducerTask extends MapReduceTask {

	public ReducerTask(String worker, BlobStorage storage, String jobClassBlob, String inputPrefix, String outputPrefix) {
		super(worker, storage, jobClassBlob, inputPrefix, outputPrefix);

	}
	
	public void execute( BlobWriter writer ) {
		JobInstance job = Jobs.newJobInstance(storage, jobClassBlob);

		String reduceKey = inputPrefix.substring( inputPrefix.lastIndexOf('-')+1);
		
		job.instance.setYielder((k, v) -> writer.writeLine(k + "\t" + v));
		job.instance.reduce_init();

		Object key = JSON.decode(Base58.decode(reduceKey), job.reducerKeyType());
		System.out.println("Word being processed " + key);
		Iterator<JsonDecoder> valuesIterators = storage.listBlobs( inputPrefix ).stream()
			.map( name -> new JsonDecoder(storage.readBlob(name).iterator(), job.reducerValueType()))
			.collect( Collectors.toList() ).iterator();
		
		job.instance.reduce(key, () -> Iterators.concat(valuesIterators));
		job.instance.reduce_end();

		storage.deleteBlobs(inputPrefix);
		writer.close();
	}
}
