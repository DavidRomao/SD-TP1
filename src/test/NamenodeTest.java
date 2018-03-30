package test;

import api.multicast.Multicast;
import server.storage.NamenodeServer;

import java.util.List;

public class NamenodeTest {

    public static void main(String[] args) {
        Multicast multicast = new Multicast();
        List<String> answer = multicast.send(NamenodeServer.NAMENODE.getBytes(),1000);
        System.out.println("answer = " + answer);
        System.exit(0);
    }

}
