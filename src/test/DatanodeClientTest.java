package test;

import sys.storage.DatanodeClient;

import java.net.URI;

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
    /*
    void deleteBlock() throws InterruptedException {
        Datanode client = new DatanodeClient(URI.create("http://0.0.0.0:9999/v1/datanode"));
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
        String id = client.createBlock("hello It is working".getBytes());
        System.out.println(id);
//        client.deleteBlock(id);
//        System.out.println();
//        Thread.sleep(1000);
//        System.out.println("Read result " +  new String(client.readBlock(id)));
//        client.delete("endless");
    }

    void readBlock() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
        List<String> blocks2 = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for this test are endless");
        }
        client.create("endless",blocks);
        client.update("endless",blocks2);
        List<String> endless = client.read("endless");
        assertEquals(endless,blocks2);
    }
	*/
    public static void main(String[] args) {
    	createBlock();
    }
}
