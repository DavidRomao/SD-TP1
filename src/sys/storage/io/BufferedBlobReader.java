package sys.storage.io;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;
import sys.storage.DatanodeClient;

import java.net.URI;
import java.util.*;

/*
 * 
 * Implements BlobReader.
 * 
 * Allows reading or iterating the lines of Blob one at a time, by fetching each block on demand.
 * 
 * Intended to allow streaming the lines of a large blob (with mamy (large) blocks) without reading it all first into memory.
 */
public class BufferedBlobReader implements BlobReader {

	final String name;
	final Namenode namenode; 
	final Map<String, Datanode> datanodes;
	
	final Iterator<String> blocks;

	final LazyBlockReader lazyBlockIterator;
	
	public BufferedBlobReader( String name, Namenode namenode, Map<String,Datanode> datanodes) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = new LinkedHashMap<>();
		
		this.blocks = this.namenode.read( name ).iterator();
		this.lazyBlockIterator = new LazyBlockReader();
	}
	
	@Override
	public String readLine() {
		return lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null ;
	}
	
	@Override
	public Iterator<String> iterator() {
		return lazyBlockIterator;
	}
	
	private Iterator<String> nextBlockLines() {
		if( blocks.hasNext() )
			return fetchBlockLines( blocks.next() ).iterator();
		else 
			return Collections.emptyIterator();
	} 

	private List<String> fetchBlockLines(String block) {
		URI uri = URI.create(block);
		String key = String.format("%s:%s", uri.getHost(), uri.getPort());
		Datanode datanode = datanodes.get(key);
		if (datanode == null) {
			datanode = new DatanodeClient(URI.create(String.format("http://"+key+"/datanode")));
			datanodes.put(key,datanode);
		}
		byte[] data = datanode.readBlock(block.substring(block.lastIndexOf("/")+1));
		return Arrays.asList( new String(data).split("\\R"));
	}
	
	private class LazyBlockReader implements Iterator<String> {
		
		Iterator<String> lines;
		
		LazyBlockReader() {
			this.lines = nextBlockLines();
		}
		
		@Override
		public String next() {
			return lines.next();
		}

		@Override
		public boolean hasNext() {
			return lines.hasNext() || (lines = nextBlockLines()).hasNext();
		}	
	}
}

