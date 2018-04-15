package test.blobstorage.Namenode;

import org.junit.jupiter.api.Test;
import sys.storage.NamenodeClient;

import java.util.ArrayList;
import java.util.List;


class NamenodeClientTest {

    public static void main(String[] args) {
    }
    @Test
    void list() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("Strings for test are endless");
        }
        client.create("endless",blocks);
        client.create("endolas",blocks);
        List<String> endolas = client.list("end");
        for (String s : endolas) {
            System.out.println(s);
        }
//        assertEquals(endolas,blocks);
    }
    @Test
    void create_read() throws InterruptedException {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        blocks.add("http://localhost:9999/datanode/");
        for (int i = 0; i < 100; i++) {
            System.out.println("Blob " + i + "/"+100);
            client.create("blobname"+i,blocks);
            Thread.sleep(20);
        }
        System.out.printf("size %d ", client.list("").size());
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
    @Test
    void update() {
        NamenodeClient client = new NamenodeClient();
        List<String> blocks = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks.add("http://localhost:9998/datanode/1234567");
        }
        List<String> blocks2 = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            blocks2.add("http://localhost:9998/datanode/12346");
        }
        client.create("endless",blocks);
        client.update("endless",blocks2);
    }

}