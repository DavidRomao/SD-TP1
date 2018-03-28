package test;

import java.net.URI;

/**
 * @author David Romao 49309
 */
public class URItest {
    public static void main(String[] args) {
        URI uri = URI.create("http://0.0.0.0:9999/v1/datanode");
        String host = uri.getHost();
        String path = uri.getPath();
        String id = uri.getPath().split("/")[2];
        System.out.println("id = " + id);
        System.out.println("path = " + path);

    }
}
