package api.storage;

import java.util.List;

public interface BlobStorage {

	List<String> listBlobs( String prefix );
	
	void deleteBlobs( String prefix );
	
	BlobReader readBlob( String name  );
	
	BlobWriter blobWriter( String name );

	Namenode getNamenode();
	
	/*
	//TODO: Might be needed for MapReduce
	void MapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize);
	*/

	interface BlobReader extends Iterable<String> {
		String readLine();
	}
	
	interface BlobWriter {
		void writeLine(String line);
		void close();
	}
}
