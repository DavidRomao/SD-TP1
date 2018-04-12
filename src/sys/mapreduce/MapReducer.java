package sys.mapreduce;

import api.mapreduce.MapReduce;

import java.io.Serializable;

abstract public class MapReducer<MK, MV, RK, RV>  implements MapReduce<MK, MV, RK, RV> , Serializable {

	private static final long serialVersionUID = -6849794470754667710L;
		
	@Override
	public void map_init() {}
	
	@Override
	public void map(MK key, MV val) {
		yield( key, val );
	}
	
	@Override
	public void map_end() {		
	}
	
	@Override
	public void reduce_init() {		
	}
	
	@Override
	public void reduce(RK key, Iterable<RV> values) {
		Thread.dumpStack();
		values.forEach( value -> yield( key, value ));
	}

	@Override
	public void reduce_end() {		
	}
		
	
	final protected <K, V> void yield(K key, V val) {
		yielder.yield(key, val);
	}
	
	public final void setYielder( Yielder yielder ) {
		this.yielder = yielder;
	}

	
	public interface Yielder {
		void yield( Object key, Object val );
	}
	
	private Yielder yielder;
	
}
