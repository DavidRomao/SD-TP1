package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.mapreduce.ComputeNode.InvalidArgumentException;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import java.net.MalformedURLException;
import java.net.URL;

//import api.ws.*;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class ComputeNodeClient {
	
	ComputeNode computeNode;
	
	public ComputeNodeClient() throws MalformedURLException {
		QName QNAME = new QName(ComputeNode.NAMESPACE, ComputeNode.NAME);
		Service service = Service.create( new URL("http://192.168.1.15:3333/mapreduce/?wsdl"), QNAME);
		computeNode = service.getPort( ComputeNode.class );
		System.out.println(  computeNode.getClass());

	}

	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InterruptedException {
		try{
			computeNode.mapReduce(jobClassBlob, inputPrefix, outputPrefix, outPartSize);
		}catch(InvalidArgumentException e){
			//Enviar um status expecifico?
		}
	}

}
