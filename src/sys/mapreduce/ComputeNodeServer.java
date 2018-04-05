package sys.mapreduce;

import api.mapreduce.ComputeNode;

public class ComputeNodeServer implements ComputeNode {
    @Override
    public void mapper(String jobClassBlob, String inputPrefix, String outputPrefix) {

    }

    @Override
    public void reducer(String jobClassBlob, String inputPrefix, String outputPrefix) {

    }

    @Override
    public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outPartSize) throws InvalidArgumentException {

    }

    public static void main(String[] args) {

    }
}
