package test.blobstorage.Datanode;

import api.storage.BlobStorage;
import sys.mapreduce.Jobs;
import sys.storage.BlobStorageClient;
import sys.storage.DatanodeClient;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

//import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatanodeClientTest {

    public static final java.lang.String STRINGS_FOR_TEST_ARE_ENDLESS = "Strings for test are endless";

    public static void createBlock() {
        DatanodeClient client = new DatanodeClient(URI.create("http://0.0.0.0:9999/v1/datanode"));
        byte[] block = STRINGS_FOR_TEST_ARE_ENDLESS.getBytes();
        String id = client.createBlock(block,"blob");
        System.out.println(id);
        byte[] bytes = client.readBlock(id.split("/")[5]);
        String string = new String(bytes, 0, STRINGS_FOR_TEST_ARE_ENDLESS.length());
        System.out.println(string);
        client.deleteBlock(id.split("/")[5]);
        bytes = client.readBlock(id.split("/")[5]);
        try {
        string = new String(bytes, 0, STRINGS_FOR_TEST_ARE_ENDLESS.length());
        System.out.println(string);
        }catch(StringIndexOutOfBoundsException e) {
        	System.out.println("There is nothing there why are you trying to read it?");
        }
//        assertEquals(STRINGS_FOR_TEST_ARE_ENDLESS,string);
//        System.out.println(client.readBlock());
    }

    private static void read(){
        DatanodeClient client = new DatanodeClient(URI.create("http://0.0.0.0:9999/datanode"));
        String block1 = client.createBlock("this is a block data".getBytes(),"blob");
        byte[] block = client.readBlock(block1.substring(block1.lastIndexOf("/")+1));
        String s = new String(block);
        System.out.println(s);
        System.out.println("block1 = " + block1);
    }

    private static void mapper() throws IOException {

        BlobStorage storage = new BlobStorageClient();

        //2. Copy all lines of WordCount.java to a blob named WordCount.
        BlobStorage.BlobWriter src = storage.blobWriter("WordCount");
        Files.readAllLines(new File("WordCount.java").toPath())
                .stream().forEach( src::writeLine );
        src.close();

        //3. Do same to files doc-1 and doc-2
        for( String doc : new String[] {"doc-1", "doc-2"}) {
            BlobStorage.BlobWriter out = storage.blobWriter(doc);
            Files.readAllLines(new File(doc + ".txt").toPath()).stream().forEach( out::writeLine );
            out.close();
        }
        DatanodeClient client = new DatanodeClient(URI.create("http://localhost:9999/datanode"));


        client.mapper(storage.getNamenode().list("doc"),"WordCount","doc-1","testOut-");
    }
    public static void main(String[] args) throws IOException {
//    	createBlock();
//    	read();
        mapper();
    }
}
