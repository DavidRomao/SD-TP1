package api.multicast;

import java.net.UnknownHostException;

public class PingReceiver implements Runnable{
	
	private final Multicast multicast;
	private String answer;
	private String expected;

	public PingReceiver(String answer,String expected) {
		this.expected = expected;

		this.multicast = new Multicast();
	    this.answer = answer;
	}
	
	@Override
	public void run() {
			try {
				multicast.receive(answer,expected);
				} catch (UnknownHostException e) {
					System.err.println("Multicast ip not found.");
				}
			}
	}
