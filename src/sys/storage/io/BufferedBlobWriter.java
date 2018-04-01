package sys.storage.io;

import api.storage.BlobStorage.BlobWriter;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.IO;

import java.io.ByteArrayOutputStream;
import java.util.*;

/*
 * 
 * Implements a ***centralized*** BlobWriter.
 * 
 * Accumulates lines in a list of blocks, avoids splitting a line across blocks.
 * When the BlobWriter is closed, the Blob (and its blocks) is published in the Namenode.
 * 
 */
public class BufferedBlobWriter implements BlobWriter {
	final String name;
	final int blockSize;
	final ByteArrayOutputStream buf;

	final Namenode namenode; 
	private final Map<String, Datanode> datanodes;
	private Datanode currentDatanode;
	final List<String> blocks = new LinkedList<>();
	private Iterator<String> keyIterator;
	private final Set<String> keys;
	
	public BufferedBlobWriter(String name, Namenode namenode, Map<String,Datanode> datanodes, int blockSize ) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		this.keys = datanodes.keySet();
		this.keyIterator = keys.iterator();
		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
		currentDatanode = getNextDatanode();
	}

	private Datanode getNextDatanode(){
		String key;
		if (keyIterator.hasNext())
			key= keyIterator.next();
		else {
			keyIterator = keys.iterator();
			key= keyIterator.next();
		}
		currentDatanode = datanodes.get(key);
		return currentDatanode;
	}

	private void flush( byte[] data, boolean eob ) {
		blocks.add( currentDatanode.createBlock(data)  );
		if( eob ) {
			namenode.create(name, blocks);
			blocks.clear();
			getNextDatanode();
		}
	}

	@Override
	public void writeLine(String line) {
		if( buf.size() + line.length() > blockSize - 1 ) {
			this.flush(buf.toByteArray(), false);
			buf.reset();
		}
		IO.write( buf, line.getBytes() );
		IO.write( buf, '\n');
	}

	@Override
	public void close() {
		flush( buf.toByteArray(), true );
		//TODO nao temos de fazer buf.reset(); ?
	}
}