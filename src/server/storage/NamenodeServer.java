package server.storage;

import api.storage.Namenode;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author David Romao 49309
 */
public class NamenodeServer implements Namenode {
    public static final String NAMENODE = "namenode";
    private Map<String,List<String>> nametable;

    public NamenodeServer() {
        this.nametable = new HashMap<>(1000);
    }

    @Override
    public List<String> list(String prefix) {
        List<String> names = new LinkedList<>();
        nametable.keySet().forEach(
                s->{
                    if(s.startsWith(prefix)) {
                        names.add(s);
                }
        });
        return names;
    }

    @Override
    public List<String> read(String name) {
        List<String> strings = nametable.get(name);
        if (strings== null)
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        else
            return strings;
    }

    @Override
    public void create(String name, List<String> blocks) {
        System.out.println("NamenodeServer.create");
        if (blocks.size()==0)
            throw new WebApplicationException(Response.Status.NO_CONTENT);
        else if (nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else
            nametable.put(name,blocks);
    }

    @Override
    public void update(String name, List<String> blocks) {
        if (!nametable.containsKey(name))
            throw new WebApplicationException(Response.Status.CONFLICT);
        else {
            List<String> put = nametable.put(name, blocks);
//            if (blocks.size() == 0)
//                throw new WebApplicationException(Response.Status.NO_CONTENT);
            //TODO  ask teacher if 200 OK is never returned
        }
    }

    @Override
    public void delete(String prefix) {
        System.out.println("NamenodeServer.delete");
        int i = 0;
        String [] toDelete = new String[nametable.size()];
        for (String s : nametable.keySet()) {
            if (s.startsWith(prefix)){
                toDelete[i++]=s;
            }
        }
        for (int j = 0; j < i; j++) {
            nametable.remove(toDelete[j]);
        }
        if (i == 0)throw new WebApplicationException(Response.Status.NOT_FOUND);
        else throw new WebApplicationException(Response.Status.NO_CONTENT);
    }


    
}
