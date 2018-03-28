package api.multicast;

import java.net.UnknownHostException;

public class PingReceiver implements Runnable{
	
	private final Multicast multicast;
	private String answer;
	
	public PingReceiver(String answer) {
		
		this.multicast = new Multicast();
	    this.answer = answer;
	}
	
	@Override
	public void run() {
		while (true){
			try {
				multicast.receive(answer,"datanode");
				} catch (UnknownHostException e) {
					System.err.println("Multicast ip not found.");
				}
	    }
	}
}
