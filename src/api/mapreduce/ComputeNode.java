package api.mapreduce;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.xml.ws.WebFault;

@WebService
public interface ComputeNode {
    String PATH = "/mapreduce";
    String NAME = "ComputeService";
    String NAMESPACE = "http://sd2018";
    String INTERFACE = "api.mapreduce.ComputeNode";

    @WebFault
    class InvalidArgumentException extends Exception {

        private static final long serialVersionUID = 1L;

        public InvalidArgumentException() {
            super("");
        }        
        public InvalidArgumentException(String msg) {
            super(msg);
        }
    }
    

    @WebMethod
    boolean mapReduce( String jobClassBlob, String inputPrefix , String outputPrefix, int outPartSize ) throws InvalidArgumentException, InterruptedException;
}