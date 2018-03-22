package test;

import api.multicast.Multicast;
import server.storage.NamenodeServer;

import java.net.UnknownHostException;
public class NamenodeTest {

    public static void main(String[] args) {
        api.multicast.Multicast multicast = new Multicast();
        try {
            String answer = multicast.send(NamenodeServer.NAMENODE.getBytes());
            System.out.println("answer = " + answer);
            System.exit(0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

}
