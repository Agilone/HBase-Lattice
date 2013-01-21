package coprocessor.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import java.util.TreeSet;
import java.util.Map.Entry;

import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.Writable;

import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.client.impl.scanner.RawScanResult;
import com.inadco.hbl.model.SimpleAggregateFunctionRegistry;

/**
 * Result tree returned from the coprocessor and used to order result elements.
 * 
 * TODO: change to com.inadco.datastructs.util.HeapUtils
 * 
 * @author michael
 *
 */
public class RawScanResultTree implements Writable {

	private transient TreeSet<Entry<byte[],RawScanResult>> set;
	private Map<byte[],RawScanResult> map = new HashMap<byte[],RawScanResult>();
	private static transient SimpleAggregateFunctionRegistry sim = new SimpleAggregateFunctionRegistry();
	private CompositeRawScanResultComparator comparator;
	public RawScanResultTree() {
		
	}

	public RawScanResultTree(CompositeRawScanResultComparator comp) {
		set = new TreeSet<Entry<byte[],RawScanResult>>(comp);
		comparator = comp;
	}
	
	public int size() {
		return set.size();
	}
	
	public void add(Entry<byte[],RawScanResult> entry) {
		if(map.get(entry.getValue().getGroup()) != null) {
			map.get(entry.getValue().getGroup()).mergeMeasures(entry.getValue(), sim, SliceOperation.ADD);
		} else {
			set.add(entry);
			map.put(entry.getKey(), entry.getValue());
		}		
	}
	
	public void addAll(Collection<Entry<byte[],RawScanResult>> coll) {
		for(Entry<byte[],RawScanResult> en : coll) {
			add(en);
		}
	}
	
	public Entry<byte[],RawScanResult> last() {
		try {
			return set.last();
		} catch(Exception e) {
			
		}
		return null;
	}
	
	public Entry<byte[],RawScanResult> popLast() {
		try {
			Entry<byte[],RawScanResult> last = set.pollLast();
			map.remove(last.getKey());
			return last;
		} catch(Exception e) {
			
		}
		return null;
	}
	
	public Entry<byte[],RawScanResult> popFirst() {
		try {
			Entry<byte[],RawScanResult> last = set.pollFirst();
			map.remove(last.getKey());
			return last;
		} catch(Exception e) {
			
		}
		return null;
	}
	
	public Collection<Entry<byte[],RawScanResult>> getEntries() {
		return map.entrySet();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		comparator.write(out);
		
		out.writeInt(map.size());
		for(Entry<byte[],RawScanResult> entry : map.entrySet()) {
			out.writeInt(entry.getKey().length);
			out.write(entry.getKey());
			entry.getValue().write(out);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		comparator = new CompositeRawScanResultComparator();
		comparator.readFields(in);
		set = new TreeSet<Entry<byte[],RawScanResult>>(comparator);
		
		int max = in.readInt();
		
		for(int i=0;i<max;i++) {
			byte[] key = new byte[in.readInt()];
			in.readFully(key);
			RawScanResult r = new RawScanResult();
			r.readFields(in);
			Entry<byte[],RawScanResult> e = new SimpleEntry<byte[],RawScanResult>(key,r);
			add(e);
		}
	}	
}
