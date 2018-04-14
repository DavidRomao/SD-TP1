package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
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

        AtomicInteger workCount= new AtomicInteger();

        Map<String,List<String>> blocksByDatanode = new LinkedHashMap<>(100);

        for (String s : namenode.list(inputPrefix)) {
            List<String> blobBlocks = namenode.read(s);
            // get uri for datanode
            String uri = blobBlocks.get(0).substring(0, blobBlocks.get(0).lastIndexOf("/"));

            List<String> datanodeBlocks = blocksByDatanode.get(uri);
            if (datanodeBlocks == null) {
                datanodeBlocks= new LinkedList<>();
                datanodeBlocks.addAll(blobBlocks);
                blocksByDatanode.put(uri,datanodeBlocks);
            }else
                datanodeBlocks.addAll(blobBlocks);
        }
        blocksByDatanode.forEach((String uri,List<String> blocks) -> {

            uri = blocks.get(0).substring(0,blocks.get(0).lastIndexOf("/"));

            DatanodeClient datanodeClient = datanodeClientMap.get(uri);
            System.out.println("Calling mapper on " + uri);
            if (datanodeClient != null) {
                datanodeClient.mapper( blocks,jobClassBlob, outputPrefix,"worker"+workCount.get());
            }else {
                datanodeClient = new DatanodeClient(URI.create(uri),storage);
                datanodeClient.mapper( blocks , jobClassBlob, outputPrefix,"worker"+workCount.get());
                datanodeClientMap.put(uri,datanodeClient);
            }
            workCount.getAndIncrement();
        });

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
