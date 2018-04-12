package api;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;

/**
 * @author David Romao 49309
 * @author Cláudio Pereira 47942
 */
public class RestRequests {

    public static  <T> T makeGet(Invocation.Builder request, Class<T> aClass) {
        int tries = 0;
        T response = null;
        while (response == null && tries < 5) {
            try {
                response = request.get(aClass);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

    /**
     *
     * @param request the request for the server
     * @param entity the entity to send to the server
     * @param aClass the return Class expected
     * @param <D>  the return type expected
     * @param <T> the entity type
     * @return The object received from the server
     */
    public static  <D,T> D makePost(Invocation.Builder request, Entity<T> entity, Class<D> aClass) {
        int tries = 0;
        D response = null;
        while (response == null && tries < 5) {
            try {
                response = request.post(entity,aClass);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

    /**
     * Post method but without a entity result;
     * @param request the request for the server
     * @param entity the object to send to the server
     * @param <T> the entity type
     * @return
     */
    public static <T> Response makePost(Invocation.Builder request, Entity<T> entity) {
        int tries = 0;
        Response response = null;
        while ( response== null && tries < 5) {
            try {
                response = request.post(entity);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

    public static Response makeDelete(Invocation.Builder request){
        int tries = 0;
        Response response = null;
        while (response == null && tries < 5) {
            try {
                response = request.delete();
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }

    public static <T> Response makePut(Invocation.Builder request, Entity<T> entity) {
        int tries = 0;
        Response response = null;
        while (response == null && tries < 5) {
            try {
                response = request.put(entity);
            } catch (javax.ws.rs.ProcessingException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.err.println("WARNING: Sleep interrupted");
                }
                tries++;
            }
        }
        return response;
    }
}
