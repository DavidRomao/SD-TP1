package test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import sys.storage.DatanodeClient;

public class DatanodeClientTest {

    @Test
    void createBlock() {
        DatanodeClient client = new DatanodeClient();
        byte[] block = "Strings for test are endless".getBytes();
        String id = client.createBlock(block);
        System.out.println(id);
        System.out.println(client.readBlock(id));
        assertEquals(client.readBlock(id), new String (block, 0, block.length));
        //client.deleteBlock(id);
        //sSystem.out.println(client.readBlock());
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
