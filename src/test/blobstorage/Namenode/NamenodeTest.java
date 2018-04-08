package test.blobstorage.Namenode;

import api.multicast.Multicast;
import servidor.storage.NamenodeServer;

import java.util.Set;

public class NamenodeTest {

    public static void main(String[] args) {
        Multicast multicast = new Multicast();
        Set<String> answer = multicast.send(NamenodeServer.NAMENODE.getBytes(),1000);
        System.out.println("answer = " + answer);
        System.exit(0);
    }

}
