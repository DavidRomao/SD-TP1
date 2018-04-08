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

        //2. Copy all lines of WordCount.java to a blob named WordCount.
        BlobStorage.BlobWriter src = storage.blobWriter("WordCount");
        Files.readAllLines(new File("WordCount.java").toPath())
                .stream().forEach( src::writeLine );
        src.close();

        //3. Do same to files doc-1 and doc-2
        for( String doc : new String[] {"doc-1", "doc-2"}) {
            BlobStorage.BlobWriter out = storage.blobWriter(doc);
            Files.readAllLines(new File(doc + ".txt").toPath()).stream().forEach( out::writeLine );
            out.close();
        }
        //4. Check the contents of the doc-X files are in storage.
        storage.listBlobs("doc-").stream().forEach( blob -> {
            storage.readBlob(blob).forEach( System.out::println );
        });

        //5. Make sure there are no blobs in storage whose names start with "results-"
        storage.deleteBlobs("results-");

        //6. Make an pseudo-unique prefix for our computation
        String jobID = Random.key64();
        String outputBlob = "results-" + jobID;

        // Get the webservices SOAP class
        QName QNAME = new QName(ComputeNode.NAMESPACE, ComputeNode.NAME);
        Service service = Service.create( new URL("http://localhost:3333/mapreduce/?wsdl"), QNAME);
        ComputeNode datanode = service.getPort( ComputeNode.class );
        System.out.println(  datanode.getClass() );

        //7. Perform the WordCount computation, over the two blobs named "doc-*"
        //   on the servidor
        datanode.mapReduce("WordCount","doc-",outputBlob,MAX_PARTITION_SIZE);

        //8. Check the results. The results will be written in one of more partitions of the given maximum size.
        storage.listBlobs(outputBlob).stream().forEach( blob -> {
            //Print this partition blob name.
            System.out.println(blob);
            storage.readBlob(blob).forEach( System.out::println );
        });
    }
}
