package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.mapreduce.Jobs;
import sys.mapreduce.MapReduceEngine;
import sys.mapreduce.MapReducer;
import sys.storage.BlobStorageClient;
import sys.storage.DatanodeClient;
import utils.IP;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@WebService(serviceName = ComputeNode.NAME,targetNamespace = ComputeNode.NAMESPACE,
        endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeServer2 implements ComputeNode {
    private static String URI_BASE;
    private final Map<String,DatanodeClient> datanodeClientMap = new ConcurrentHashMap<>();
    private static BlobStorage storage;
    private static Namenode namenode;
    @Override
    public boolean mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InvalidArgumentException {
        System.out.println("ComputeNodeServer2.mapReduce");

        for (String s : namenode.list(inputPrefix)) {
            List<String> blocks = namenode.read(s);
            String uri = blocks.get(0).split("datanode")[0];

            DatanodeClient datanodeClient = datanodeClientMap.get(uri);
            if (datanodeClient != null) {
                datanodeClient.mapper( blocks,jobClassBlob,s, outputPrefix );
            }else {
                datanodeClient = new DatanodeClient(URI.create(uri+Datanode.PATH),storage);
                System.out.println("Calling mapper on " + s);
                datanodeClient.mapper( blocks , jobClassBlob,s, outputPrefix );
                datanodeClientMap.put(uri,datanodeClient);
            }
        }
        datanodeClientMap.values().iterator().next().reducer(jobClassBlob, outputPrefix, outPartSize);
        return true;
//        MapReduceEngine engine = new MapReduceEngine("worker",storage);
//        engine.executeJob( jobClassBlob,inputPrefix,outputPrefix,outPartSize);

    }


    public static void main(String[] args) {
        if (args.length== 0)
            URI_BASE = String.format("http://%s:%d%s",IP.hostAddress(),3333,ComputeNode.PATH);
        else
            URI_BASE = args[0]+ComputeNode.PATH;
        storage = new BlobStorageClient();
        namenode = storage.getNamenode();

        Endpoint.publish(URI_BASE,new ComputeNodeServer2());
        System.err.println("ComputeNode Server ready at " + URI_BASE );
    }
}
