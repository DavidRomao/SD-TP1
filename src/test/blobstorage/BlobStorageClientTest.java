package test.blobstorage;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import org.junit.jupiter.api.Test;
import sys.storage.BlobStorageClient;
import sys.storage.DatanodeClient;

import java.net.URI;
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
        BlobWriter doc = storage.blobWriter("doc");
        for(int i = 0; i<50; i++) {
            doc.writeLine("bad backwards means dab. So dab your problems away");
        }
       doc.close();
//        String doc1 = storage.getNamenode().read("doc").get(0);
//        new DatanodeClient(URI.create("http://192.168.1.15:9999/datanode")).deleteBlock(doc1.substring(doc1.lastIndexOf("/")));
//        BlobReader strings = storage.readBlob("doc");
//        String s = strings.readLine();
//       System.out.println(s);
//        testWrite(100);
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
