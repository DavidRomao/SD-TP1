package test;

import api.storage.BlobStorage;
import api.storage.Datanode;
import org.junit.jupiter.api.Test;
import sys.storage.BlobStorageClient;

import javax.ws.rs.NotFoundException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class GarbageCollectorTest {

    private BlobStorage client = new BlobStorageClient();
    @Test
    public void deleteTestNamenoded() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            BlobStorage.BlobWriter blobWriter = client.blobWriter("doc" + i);
            blobWriter.writeLine("some line");
            blobWriter.close();
        }
        List<String> blocks = new LinkedList<>();
        client.listBlobs("").forEach( name -> blocks.add( client.getNamenode().read(name).get(0) ) );
        client.getNamenode().delete("");
        Datanode next = client.getDatanodesIterator().next();
        Thread.sleep(20 * 1000);
        for (String block : blocks) {
            try {
                byte[] bytes = next.readBlock(block.substring(block.lastIndexOf("/")+1));
            }catch (NotFoundException e){

            }
        }
    }

    @Test
    public void createTest() throws InterruptedException {
        Datanode next = client.getDatanodesIterator().next();
        LinkedList<String> blocks = new LinkedList<>();
        for (int i = 0; i < 20; i++) {
            String block1 = next.createBlock("Some random data".getBytes(), "block1");
            blocks.add(block1);
        }
        Thread.sleep(22*1000);
        blocks.forEach( (String s) -> System.out.println( new String (next.readBlock(s.substring(s.lastIndexOf("/") ) ) ) ) );

    }
}
