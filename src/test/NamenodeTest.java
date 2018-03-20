package test;

import api.multicast.Multicast;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.UnknownHostException;

import server.storage.NamenodeServer;
import server.storage.NamenodeServer.*;
public class NamenodeTest {

    public static void main(String[] args) {
        api.multicast.Multicast multicast = new Multicast(NamenodeServer.multicastListener);
        try {
            String answer = multicast.send(NamenodeServer.NAMENODE.getBytes());
            System.out.println("answer = " + answer);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        /*
        ClientConfig config = new ClientConfig();
        Client client = ClientBuilder.newClient(config);
        URI baseURI = UriBuilder.fromUri("http://localhost:9999/v1").build();
        WebTarget target = client.target( baseURI );

        Response response = target.path("/some-path/28s0pkung0")
                .request()
                .get();

        if( response.hasEntity() ) {
            byte[] data = response.readEntity(byte[].class);
            System.out.println( "data resource length: " + data.length );
        } else
            System.err.println( response.getStatus() );
        */
    }

}
