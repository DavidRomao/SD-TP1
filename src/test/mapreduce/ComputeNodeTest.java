package test.mapreduce;

import api.mapreduce.ComputeNode;
import api.storage.BlobStorage;
import sys.storage.BlobStorageClient;
import utils.Random;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;


public class ComputeNodeTest {

    private static final int MAX_PARTITION_SIZE = 6;

    /*
     * Executes a MapReduce computation using the BlobStorageClient implementation.
     *
     * It should be possible to run computation just by replacing the LocalBlobStorage implementation with your own (backed by the servers).
     *
     */
    public static void main(String[] args) throws Exception {
        
        //1. Get the storage implementation. Replace with your own implementation...
        BlobStorage storage = new BlobStorageClient();

        storage.deleteBlobs("");
        //2. Copy all lines of WordCount.java to a blob named WordCount.
        BlobStorage.BlobWriter src = storage.blobWriter("WordCount");
        Files.readAllLines(new File("WordCount.java").toPath())
                .stream().forEach( src::writeLine );
        src.close();

        for (int i = 0; i < 100; i++) {
            BlobStorage.BlobWriter out= storage.blobWriter("doc-"+i);
            out.writeLine("uma batata ja nao e viva");
            out.writeLine("outra puta maluca");
            out.close();
        }
//        for (int i = 0; i < 21; i++) {
//            BlobStorage.BlobWriter out= storage.blobWriter("doc-1"+i);
//            out.writeLine("outra puta maluca");
//            out.writeLine("uma vaca parva");
//            out.close();
//        }
        //printing blobs location
        System.err.println("printing blobs location");
        storage.listBlobs("doc").forEach((blob) -> storage.getNamenode().read(blob).forEach( System.out::println));
        // uma batata ja nao e viva
        // outra maluca esta no ceu

        
        //5. Make sure there are no blobs in storage whose names start with "results-"
        storage.deleteBlobs("results-");

        //6. Make an pseudo-unique prefix for our computation
        String jobID = Random.key64();
        String outputBlob = "results-" + jobID;

        // Get the webservices SOAP class
        QName QNAME = new QName(ComputeNode.NAMESPACE, ComputeNode.NAME);
//        Service service = Service.create( new URL("http://127.0.1.1:3333/mapreduce/?wsdl"), QNAME);
        Service service = Service.create( new URL("http://192.168.1.15:3333/mapreduce/?wsdl"), QNAME);
        ComputeNode computeNode = service.getPort( ComputeNode.class );
        System.out.println(  computeNode.getClass() );
        //7. Perform the WordCount computation, over the two blobs named "doc-*"
        //   on the servidor
        computeNode.mapReduce("WordCount","doc-",outputBlob,MAX_PARTITION_SIZE);

        
        //8. Check the results. The results will be written in one of more partitions of the given maximum size.
        storage.listBlobs(outputBlob).stream().forEach( blob -> {
            //Print this partition blob name.
//            System.out.println(blob);
            storage.readBlob(blob).forEach( System.out::println );
        });
    }
}
