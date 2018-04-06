package test.blobstorage.Datanode;

import api.multicast.Multicast;

import java.util.List;

public class DatanodeTest {
	
    public static void main(String[] args) {
        Multicast multicast = new Multicast();
        List<String> answer = multicast.send("datanode".getBytes(),1000);
        System.out.println("answer = " + answer);
        System.exit(0);
    }
	
}
