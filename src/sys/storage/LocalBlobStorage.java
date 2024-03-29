package sys.storage;

import api.storage.BlobStorage;
import api.storage.Datanode;
import api.storage.Namenode;

import java.util.Iterator;
import java.util.List;

public class LocalBlobStorage implements BlobStorage {
	private static final int BLOCK_SIZE=512;

	Namenode namenode;
	Datanode[] datanodes;

	public LocalBlobStorage() {
		this.namenode = new NamenodeClient();
//		this.datanodes = new Datanode[] { new DatanodeClient() };
	}

	@Override
	public Iterator<Datanode> getDatanodesIterator() {
		return null;
	}

	@Override
	public List<String> listBlobs(String prefix) {
		return namenode.list(prefix);
	}

	@Override
	public void deleteBlobs(String prefix) {
		namenode.list( prefix ).forEach( blob -> {
			namenode.read( blob ).forEach( block -> {
				datanodes[0].deleteBlock(block);
			});
		});
		namenode.delete(prefix);
	}

	@Override
	public BlobReader readBlob(String name) {
//		return new BufferedBlobReader( name, namenode, datanodes[0]);
		return null;
	}

	@Override
	public BlobWriter blobWriter(String name) {
//		return new BufferedBlobWriter( name, namenode, datanodes, BLOCK_SIZE);
		return null;
	}

	@Override
	public Namenode getNamenode() {
		return null;
	}

	/*
    //TODO: Just commented to assure this is the right path
	@Override
	public void MapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) {
		// TODO Just a Stub
	}
	*/
}
