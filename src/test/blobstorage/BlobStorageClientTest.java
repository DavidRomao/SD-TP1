package test.blobstorage;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import org.glassfish.hk2.api.messaging.SubscribeTo;
import org.junit.jupiter.api.Test;
import sys.storage.BlobStorageClient;

import java.util.List;

import static api.storage.BlobStorage.BlobReader;
import static java.lang.System.out;

/**
 * @author David Romao 49309
 */
public class BlobStorageClientTest {

    static BlobStorage storage;

    public static void main(String[] args) {
        storage = new BlobStorageClient();
        testWrite(100);
//        testRead(100);

//        test1();
    }

    private static void test1(){

        BlobStorageClient blobstorage = new BlobStorageClient();
        BlobWriter blobWriter = blobstorage.blobWriter("doc1.txt");
        blobWriter.writeLine("hello i am the first blob ever");
        blobWriter.writeLine("hello i am the first blob ever again");
        blobWriter.close();
        List<String> doc = blobstorage.listBlobs("doc1");
        out.println("doc size " + doc.size());
        for (String s : doc) {
            out.println(s);
            BlobReader reader = blobstorage.readBlob(s);
            for (String s1 : reader) {
                out.println(s1);
            }
        }
    }

    @SuppressWarnings("Duplicates")
    private static void testWrite(int k){

        for (int i = 0; i < k ; i++) {

            BlobWriter blobWriter = storage.blobWriter("Blob"+i);
            for (int j = 0; j < 10; j++) {
                blobWriter.writeLine("Content");
            }
            blobWriter.close();
        }

    }

    @Test
    private static void testRead(int n){
//        testWrite(10);

        List<String> word = storage.listBlobs("Blob");
        assert word.size()== n;

        word.forEach( blobl -> storage.readBlob(blobl).readLine());
    }
}
