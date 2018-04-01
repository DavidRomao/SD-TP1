package test.blobstorage;

import api.storage.BlobStorage;
import api.storage.BlobStorage.BlobWriter;
import sys.storage.BlobStorageClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
        testRead();

    }

    private void test1(){

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

    private static void testRead(){
        testWrite();

        List<String> word = storage.listBlobs("Word");
        word.forEach(System.out::println);
        BlobReader strings = storage.readBlob(word.get(0));
//        System.out.println(strings.readLine());
                storage.listBlobs("Word").forEach( blob -> {
            storage.readBlob( blob ).forEach( System.out::println );
        });
    }
}
