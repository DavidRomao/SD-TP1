package servidor.storage;

import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.DatanodeClient;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author Cláudio Pereira 47942
 * @author David Romao 49309
 */
public class NamenodeServer implements Namenode {
    public static final String NAMENODE = "Namenode";
    private ConcurrentMap<String,List<String>> nametable;
    private ConcurrentMap<String,List<String>> suspects;
    private ConcurrentMap<String,Datanode> datanodes;
    public NamenodeServer() {
        this.nametable = new ConcurrentHashMap<>(10000);
        this.suspects = new ConcurrentHashMap<>(10000);
        this.datanodes = new ConcurrentHashMap<>();
        /*
        Send to the datanode the blocks recently deleted from the namenode table
         */
        /// TODO: 14/04/18 fix gargabe collector
		GarbageCollector garbageCollector = new GarbageCollector();
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(garbageCollector, 50, 50, TimeUnit.SECONDS);
//        new Thread(new GargageCollector()).start();
    }
    public class GarbageCollector implements Runnable{

        @Override
        public void run() {
//            while (true){
            List<String> toRemove = new LinkedList<>();
            System.err.println(" GARBAGE COLLECTOR \n Suspects size : " + suspects.size());
            for (String blobName : suspects.keySet()) {
                List<String> blocks = suspects.get(blobName);
                String uri = blocks.get(0).substring(0, blocks.get(0).lastIndexOf("/"));
                try {
                    Datanode datanode = datanodes.get(uri);
                    if (datanode == null) {
                        datanode = new DatanodeClient(URI.create(uri));
                        datanodes.put(uri, datanode);
                    }
                    // extract block id from complete uri
                    datanode.confirmDeletion(blocks.stream().map((s) -> s.substring(s.lastIndexOf("/") + 1)).collect(Collectors.toList()));
                } catch (StringIndexOutOfBoundsException e) {
                    System.err.println("");
                }
                toRemove.add(blobName);
            }
                    // remove from the suspects the ones who were sent to the datanodes
                    // because there is chance that the table changed since the loop ended and
                    // a new blob was added before we removed all the old blobs
                    toRemove.forEach( suspects::remove );
            }
    }

    @Override
    public List<String> list(String prefix) {
        System.err.println("Collecting blobs with prefix : " + prefix);
        List<String> names = new LinkedList<>();
        if (prefix.equals(""))
            names.addAll(nametable.keySet());
        else
            nametable.keySet().forEach(
                s->{
                    if(s.startsWith(prefix)) {
                        names.add(s);
                }
        });
        System.err.println("Collected " + names.size());
//        System.err.println("Printing blocks uri");
//        names.forEach( name -> nametable.get(name).forEach(System.err::println));
        return names;
    }

    @Override
    public List<String> read(String name) {
        System.err.println("NamenodeServer.read");
        System.err.println("blob name " + name);
        List<String> strings = nametable.get(name);
        if (strings== null)
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        else
            return strings;
    }

    @Override
    synchronized public void create(String name, List<String> blocks) {
        System.err.println("NamenodeServer.create");
        System.err.println("Created " + name);
       if (nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else {
            nametable.put(name,blocks);
            // validate the blocks stored on the datanodes, to prove they are not forgotten
            // as each blob is only stored in a datanode we can send the complete list just to one datanode
            // http://0.0.0.0:9999/datanode/blockid
           try{

               String uri = blocks.get(0);
               String ip_port_datanode = uri.substring(0, uri .lastIndexOf("/"));

               Datanode datanode = datanodes.get(ip_port_datanode);
               if (datanode == null) {
                   datanode = new DatanodeClient(URI.create(ip_port_datanode));
                   datanodes.put(ip_port_datanode,datanode);
               }
               Datanode finalDatanode = datanode;
               new Thread( () -> finalDatanode.confirmBlocks(blocks)).start();

           }catch (IndexOutOfBoundsException e){
               System.err.println(" Index out of bounds exception");
           }
           throw new WebApplicationException(Response.Status.NO_CONTENT);
       }
    }

    @Override
    synchronized public void update(String name, List<String> blocks) {
        System.out.println("NamenodeServer.update");
        if (!nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else {
            List<String> put = nametable.put(name, blocks);
            throw new WebApplicationException(Response.Status.NO_CONTENT);
        }
    }

    @Override
    synchronized public void delete(String prefix) {
        System.out.println("NamenodeServer.delete");
        int i = 0;
        String [] toDelete = new String[nametable.size()];
        System.err.println("nametable size " + nametable.size());
        for (String s : nametable.keySet()) {
            if (s.startsWith(prefix)){
                toDelete[i++]=s;
            }

            // for garbage collector, make sure the block does not exist after some time
            System.err.println(" get " + s + " ");
            List<String> blocks = nametable.get(s);
            System.err.println(" blocks size  " + blocks.size());
            // datanode uri
            suspects.put(s , blocks);
            System.err.println("PUT DONE");
        }
        for (int j = 0; j < i; j++) {
            nametable.remove(toDelete[j]);
        }
        System.err.println("Almost done");
        if (i == 0)throw new WebApplicationException(Response.Status.NOT_FOUND);
        else throw new WebApplicationException(Response.Status.NO_CONTENT);
    }

    @Override
    public boolean exists(String name, String block) {
        try{
            return nametable.get(name).indexOf(block) != -1;
        }catch (NullPointerException e){
            return false;
        }
    }

    @Override
    public boolean exists(String block) {
        final boolean[] exist = {false};
        nametable.forEach( (String key, List<String> list) -> {
            for (String id : list) {
                if (id.contains(block)) {
                    exist[0] = true;
                    return; // end forEach loop , similar to break
                }
            }
        } );
        return exist[0];
    }


}
