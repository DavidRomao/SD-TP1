package servidor.mapreduce;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import api.storage.Namenode;
import sys.mapreduce.MapReduceEngine;
import sys.storage.BlobStorageClient;
import utils.Random;

import javax.jws.WebService;
import javax.xml.ws.Endpoint;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
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
    	/*
    	BlobStorage storage = new BlobStorageClient() ;
        MapReduceEngine engine = new MapReduceEngine("local",storage);
        Map<String,List<String>> blocksByDatanode = new HashMap<>();
        Namenode namenode = storage.getNamenode();
        storage.listBlobs(inputPrefix).forEach( blob -> {
            namenode.read(blob).forEach( block -> {
                String ip_path = block.split("//")[1];
                String ip = ip_path.substring(0,ip_path.indexOf("/"));
                List<String> strings = blocksByDatanode.get( ip );
                if (strings != null)
                    strings.add( block.substring( block.lastIndexOf("/")+1) );
                else {
                    LinkedList<String> list = new LinkedList<>();
                    //Split example to get block
                    //e.g. localhost:9999/datanode/gupf3494lo
                    //0- localhost:9999
                    //1- datanode
                    //2- gupf3494lo
                    
                    list.add(ip_path.split("/")[2]);
                    blocksByDatanode.put(ip,list);
                }

            });
        });
        // print the blocks by datanode
        blocksByDatanode.keySet().forEach( datanode-> {
            System.out.println("datanode ip : " + datanode);
            blocksByDatanode.get(datanode).forEach(System.out::println);
            System.out.printf("\n");
        });
        */ 
    	//TODO : In my opinion this doesn't make much sense since the own mapreducer finds all blocks, calling the engine seems to be enough
    	BlobStorage storage = new BlobStorageClient() ;
    	
		MapReduceEngine engine = new MapReduceEngine( "local", storage);
        engine.executeJob(jobClassBlob,inputPrefix,outputPrefix,outPartSize);
    }
}
