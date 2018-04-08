package test.blobstorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobReader;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.BlobStorageClient;

/**
 * 
 * @author Claudio Pereira 47942
 *
 */
public class BlobStorageClientAddRemoveTest {

    static BlobStorage storage;

    public static void main(String[] args) {
        storage = new BlobStorageClient();
        testWrite();
        storage.deleteBlobs("Word");
        System.out.println("Deleted");
        testWrite();
        readBlobs("Word");

    }
	
    private static void testWrite(){
        try {
            BlobWriter writer2 = storage.blobWriter("WordCount");
            for( String line : Files.readAllLines(new File("WordCount.java").toPath()))
                writer2.writeLine( line );
            writer2.close();
        } catch (IOException e) {
            System.err.println("File not found");
        }
    }
    
    private static void readBlobs(String word){
    	List<String> lWord = storage.listBlobs(word);
    	lWord.forEach(System.out::println);
        BlobReader strings = storage.readBlob(lWord.get(0));
        System.out.printf("Line: %s", strings.readLine());
                storage.listBlobs("Word").forEach( blob -> {
            storage.readBlob( blob ).forEach( System.out::println );
        });
    }
}
