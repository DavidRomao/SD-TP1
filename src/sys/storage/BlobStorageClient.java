package sys.storage;

import api.multicast.Multicast;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.io.BufferedBlobReader;
import sys.storage.io.BufferedBlobWriter;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author David Romao 49309
 * @author retard number *****
 */
public class BlobStorageClient implements api.storage.BlobStorage{

    private static final int BLOCK_SIZE=512;

    private Namenode namenode;
    private Map<String,Datanode> datanodes;

    public BlobStorageClient() {
        namenode = new NamenodeClient();
        datanodes = new HashMap<>();
        discover(); // discover namenode servers
    }

    private void discover(){
        Multicast multicast = new Multicast();
        Set<String> send = multicast.send("datanode".getBytes(), 500);
        for (String s : send) {
            System.err.println(s);
            URI uri = URI.create(s);
            datanodes.put(String.format("%s:%s",uri.getHost(),uri.getPort()),new DatanodeClient(uri));
        }
//        http://0.0.0.0:9999/v1/datanode
    }

    @Override
    public List<String> listBlobs(String prefix) {
        List<String> list = namenode.list(prefix);
        return list;
    }

    @Override
    public void deleteBlobs(String prefix) {
        List<String> docs = namenode.list(prefix);
        // http://localhost:8888/v1/id
        for (String doc : docs) {
            List<String> blocks = namenode.read(doc);
            blocks.forEach( s-> {
                URI uri = URI.create(s);
                String host = uri.getHost();
                Datanode datanode = datanodes.get(String.format("%s:%s",host,uri.getPort()));
                String id = uri.getPath().split("/")[2];
                datanode.deleteBlock(id);

            });
        }
        namenode.delete(prefix);
    }

    @Override
    public BlobReader readBlob(String name) {
        return new BufferedBlobReader(name,namenode,datanodes);
    }

    @Override
    public BlobWriter blobWriter(String name) {
        return new BufferedBlobWriter(name,namenode,datanodes,BLOCK_SIZE);
    }

    @Override
    public Namenode getNamenode() {
        return namenode;
    }
}
