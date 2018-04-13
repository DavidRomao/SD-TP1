package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;
import jersey.repackaged.com.google.common.collect.Lists;
import sys.storage.BlobStorageClient;
import sys.storage.DatanodeClient;
import utils.IP;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        int workCount=0;
        for (String s : namenode.list(inputPrefix)) {
            List<String> blocks = namenode.read(s);
            String uri = blocks.get(0).split("datanode")[0];

            DatanodeClient datanodeClient = datanodeClientMap.get(uri);
            if (datanodeClient != null) {
                datanodeClient.mapper( blocks,jobClassBlob,s, outputPrefix,"worker"+workCount);
            }else {
                datanodeClient = new DatanodeClient(URI.create(uri+Datanode.PATH),storage);
                System.out.println("Calling mapper on " + s);
                datanodeClient.mapper( blocks , jobClassBlob,s, outputPrefix,"worker"+workCount);
                datanodeClientMap.put(uri,datanodeClient);
            }
            workCount++;
        }
//        datanodeClientMap.values().iterator().next().reducer(jobClassBlob, outputPrefix, outPartSize);

        storage.listBlobs(outputPrefix + "-map-").stream().forEach( blob -> System.out.println(blob));
        Set<String> reduceKeyPrefixes = storage.listBlobs(outputPrefix + "-map-").stream()
                .map( blob -> blob.substring( 0, blob.lastIndexOf('-')))
                .collect( Collectors.toSet() );

        AtomicInteger partitionCounter = new AtomicInteger();
        Set<String> datanodes = datanodeClientMap.keySet();
        Iterator<String> iterator = datanodes.iterator();
        for (List<String> partitionKeyList : Lists.partition(new ArrayList<>(reduceKeyPrefixes), outPartSize)) {

                for (String keyPrefix : partitionKeyList) {

                    if (iterator.hasNext())
                        datanodeClientMap.get(iterator.next()).reducer(keyPrefix, jobClassBlob, outputPrefix, outPartSize, partitionCounter.get());
                    else {
                        iterator = datanodes.iterator();
                        datanodeClientMap.get(iterator.next()).reducer(keyPrefix, jobClassBlob, outputPrefix, outPartSize, partitionCounter.get());
                    }
                    partitionCounter.getAndIncrement();
//                new ReducerTask("client", storage, jobClassBlob, keyPrefix, outputPrefix).execute(writer);
                }
            }

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
