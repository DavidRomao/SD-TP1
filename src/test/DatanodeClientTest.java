package test;

import sys.storage.DatanodeClient;
import sys.storage.NamenodeClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
        DatanodeClient client = new DatanodeClient(URI.create("http://0.0.0.0:9999/v1/datanode"));
        byte[] block = client.readBlock("e0d7bed7pu");
        String s = new String(block);
        System.out.println(s);
    }
    public static void main(String[] args) {
//    	createBlock();
    	read();
    }
}
