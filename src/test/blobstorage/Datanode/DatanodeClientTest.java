package test.blobstorage.Datanode;

import sys.storage.DatanodeClient;

import java.net.URI;

//import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatanodeClientTest {

    public static final java.lang.String STRINGS_FOR_TEST_ARE_ENDLESS = "Strings for test are endless";

    public static void createBlock() {
        DatanodeClient client = new DatanodeClient(URI.create("http://0.0.0.0:9999/v1/datanode"));
        byte[] block = STRINGS_FOR_TEST_ARE_ENDLESS.getBytes();
        String id = client.createBlock(block);
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
        String block1 = client.createBlock("this is a block data".getBytes());
        byte[] block = client.readBlock(block1.substring(block1.lastIndexOf("/")+1));
        String s = new String(block);
        System.out.println(s);
        System.out.println("block1 = " + block1);
    }
    public static void main(String[] args) {
//    	createBlock();
    	read();
    }
}
