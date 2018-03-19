package api.storage;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author David Romao 49309
 */
public class NamenodeImpl implements Namenode {
    private Map<String,List<String>> nametable;

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
        if (nametable.containsKey(name))
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
            throw new WebApplicationException(Response.Status.NO_CONTENT);
            //TODO  ask teacher if 200 OK is never returned
        }
    }

    @Override
    public void delete(String prefix) {
        nametable.keySet().forEach(s->{
            if (s.startsWith(prefix))
                nametable.remove(s);
        });
    }
}
