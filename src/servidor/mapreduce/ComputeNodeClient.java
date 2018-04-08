package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.mapreduce.ComputeNode.InvalidArgumentException;

//import api.ws.*;
import java.net.*;
import javax.xml.ws.*;
import javax.xml.namespace.*;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class ComputeNodeClient {
	
	ComputeNode computeNode;
	
	public ComputeNodeClient() throws MalformedURLException {
		QName QNAME = new QName(ComputeNode.NAMESPACE, ComputeNode.NAME);
		Service service = Service.create( new URL("http://0.0.0.0:3333/mapreduce/?wsdl"), QNAME);
		ComputeNode computeNode = service.getPort( ComputeNode.class );
		System.out.println(  computeNode.getClass());

	}

	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) {
		try{
			computeNode.mapReduce(jobClassBlob, inputPrefix, outputPrefix, outPartSize);
		}catch(InvalidArgumentException e){
			//Enviar um status expecifico?
		}
	}

}
