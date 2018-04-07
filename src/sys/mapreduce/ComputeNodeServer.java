package sys.mapreduce;

import api.mapreduce.ComputeNode;
import sys.storage.BlobStorageClient;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;

@WebService(serviceName = ComputeNode.NAME,targetNamespace = ComputeNode.NAMESPACE,
        endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeServer implements ComputeNode{
    private static String URI_BASE;

    public static void main(String[] args) {

        if (args.length== 0)
            URI_BASE = "http://0.0.0.0:3333" + ComputeNode.PATH;
        else
            URI_BASE = args[0]+ComputeNode.PATH;
        //does nothing, just placeholder
        System.err.println("ComputeNode Server ready at " + URI_BASE );
        Endpoint.publish(URI_BASE,new ComputeNodeServer());

    }


    @Override
    public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InvalidArgumentException {
        MapReduceEngine engine = new MapReduceEngine("local",new BlobStorageClient());
        engine.executeJob(jobClassBlob,inputPrefix,outputPrefix,6);
    }
}
