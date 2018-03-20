package api.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

public class Multicast {

    private final String ip;
    private final int port;

    public Multicast(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

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
                System.out.write( request.getData(), 0, request.getLength() ) ;

                //prepare and send reply... (unicast)
                if (new String(request.getData(),0,request.getLength()).equals(expected)) {
                    System.out.println("Message received");
                    byte[] data = answer.getBytes();
                    DatagramPacket response = new DatagramPacket(data, data.length);
                    response.setAddress(request.getAddress());
                    response.setPort(request.getPort());
                    System.out.println( "Address :" +request.getAddress());
                    System.out.println("Port : " + request.getPort());
                    socket.send(response);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String send(byte[] data) throws UnknownHostException {
        final InetAddress group = InetAddress.getByName( this.ip) ;
        String reply="";
        if( ! group.isMulticastAddress()) {
            System.out.println( "Not a multicast address (use range : 224.0.0.0 -- 239.255.255.255)");
        }

        try(MulticastSocket socket = new MulticastSocket()) {
            DatagramPacket request = new DatagramPacket( data, data.length, group, port ) ;
            socket.send( request ) ;
            // receive reply (unicast)
            DatagramPacket packet = new DatagramPacket(new byte[1024],1024);
            socket.receive(packet);
            reply=new String( packet.getData(),0,packet.getLength() );
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reply;
    }
}
