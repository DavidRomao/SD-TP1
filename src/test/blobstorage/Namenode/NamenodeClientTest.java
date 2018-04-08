package test.blobstorage.Namenode;

import org.junit.jupiter.api.Test;
import sys.storage.NamenodeClient;
import utils.Random;

import java.util.ArrayList;
import java.util.List;


class NamenodeClientTest {

    @Test
    void list() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
//        client.create("endless",blocks);
//        client.create("endolas",blocks);
        List<String> endolas = client.list("end");
        for (String s : endolas) {
            System.out.println(s);
        }
//        assertEquals(endolas,blocks);
    }
    @Test
    void create_read() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 10; i++) {
            blocks.add("Strings for test are endless");
        }
        for (int i = 0; i < 10000; i++) {
            client.create(Random.key64(),blocks);
        }

        //        assertEquals(endless,blocks);
    }

    @Test
    void delete() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
        client.create("endless",blocks);

        client.delete("endless1");
    }

    void update() {
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
    }

}