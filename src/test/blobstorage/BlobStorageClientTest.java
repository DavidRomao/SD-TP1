package test.blobstorage;

import api.storage.BlobStorage.BlobReader;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.BlobStorageClient;

import java.util.List;

/**
 * @author David Romao 49309
 */
public class BlobStorageClientTest {

    public static void main(String[] args) {
        BlobStorageClient blobstorage = new BlobStorageClient();
        BlobWriter blobWriter = blobstorage.blobWriter("doc1.txt");
        blobWriter.writeLine("hello i am the first blob ever");
        blobWriter.close();
        List<String> doc = blobstorage.listBlobs("");
        System.out.println("doc size " + doc.size());
        for (String s : doc) {
            System.out.println(s);
        }
        BlobReader reader = blobstorage.readBlob("doc1.txt");
		for( String line : reader )
			System.out.println( line  );

    }
}
