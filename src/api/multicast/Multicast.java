package api.multicast;

import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.Set;

public class Multicast {

    private final String ip;
    private final int port;
    private static final URI defaultURI = URI.create("http://225.100.100.100:8080");
    public Multicast() {
        this.ip = defaultURI.getHost();
        this.port = defaultURI.getPort();
    }


    public Multicast(URI uri) {
        this.ip = uri.getHost();
        this.port = uri.getPort();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void receive(String answer, String expected) throws UnknownHostException {
        final int MAX_DATAGRAM_SIZE = 65536;
        final InetAddress group = InetAddress.getByName( this.ip) ;
        if( ! group.isMulticastAddress()) {
            System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
            System.exit( 1);
        }

        try( MulticastSocket socket = new MulticastSocket( this.port )) {
            socket.joinGroup( group);
            while( true ) {
                byte[] buffer = new byte[MAX_DATAGRAM_SIZE] ;
                DatagramPacket request = new DatagramPacket( buffer, buffer.length ) ;
                socket.receive( request );
                //System.out.write( request.getData(), 0, request.getLength() ) ;
                
                //prepare and send reply... (unicast)
                String requestS = new String(request.getData(),0,request.getLength());
                if (requestS.equalsIgnoreCase(expected)) {
                	//System.out.println("Message received");
                    byte[] data = answer.getBytes();
                    DatagramPacket response = new DatagramPacket(data, data.length);
                    response.setAddress(request.getAddress());
                    response.setPort(request.getPort());
                    try {
                        socket.send(response);
                    }catch (java.io.IOException e){
                        System.err.println("Failed to send message");
                        System.err.println(request.getAddress());
                        System.err.println(request.getPort());
                    }
                    System.err.println("Multicast.receive");
                    System.err.println( "Address :" +request.getAddress());
                    System.err.println("Port : " + request.getPort());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public Set<String> send(byte[] data,int timeout)  {

        Set<String> replies = new HashSet<>();
        try(MulticastSocket socket = new MulticastSocket()) {
            final InetAddress group = InetAddress.getByName( this.ip) ;
            if( ! group.isMulticastAddress()) {
                System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
            }
            DatagramPacket request = new DatagramPacket( data, data.length, group, port ) ;
            socket.send( request ) ;
            int ntries = 0;
            socket.setSoTimeout(timeout);
            while (ntries < 5) {
                try {
                    DatagramPacket datagram = new DatagramPacket(new byte[1024], 1024);
                    socket.receive(datagram);
                    replies.add(new String(datagram.getData(), 0, datagram.getLength()));
                }catch (SocketTimeoutException e){
                    socket.send( request ) ;
                    ntries+=1;
//            System.out.println("All replies received");
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
        return replies;
    }
}
