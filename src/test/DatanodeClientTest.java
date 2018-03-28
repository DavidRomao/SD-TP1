package test;

import org.junit.jupiter.api.Test;
import sys.storage.DatanodeClient;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DatanodeClientTest {

    public static final java.lang.String STRINGS_FOR_TEST_ARE_ENDLESS = "Strings for test are endless";

    @Test
    void createBlock() {
        DatanodeClient client = new DatanodeClient(URI.create("http://0.0.0.0:9999/v1/datanode"));
        byte[] block = STRINGS_FOR_TEST_ARE_ENDLESS.getBytes();
        String id = client.createBlock(block);
        System.out.println(id);
        client.deleteBlock(id);
        byte[] bytes = client.readBlock(id);
        String string = new String(bytes, 0, STRINGS_FOR_TEST_ARE_ENDLESS.length());
        System.out.println(string);
        assertEquals(STRINGS_FOR_TEST_ARE_ENDLESS,string);
//        System.out.println(client.readBlock());
    }
/*
    @Test
    void deleteBlock() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
//        client.create("endless",blocks);

        client.delete("endless");
//        client.delete("endless");
    }

    @Test
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
}
