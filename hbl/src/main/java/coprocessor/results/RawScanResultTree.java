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

import org.apache.commons.codec.binary.Base64;

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

	private static transient SimpleAggregateFunctionRegistry sim = new SimpleAggregateFunctionRegistry();
	
	private transient TreeSet<Entry<byte[],RawScanResult>> set;
	
	private Map<String,RawScanResult> map = new HashMap<String,RawScanResult>();
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
	
	public boolean containsGroup(Entry<byte[],RawScanResult> entry) {
		return (map.get(Base64.encodeBase64String(entry.getValue().getGroup())) != null);
		/*for(Entry<String,RawScanResult> ent : map.entrySet()) {
			if(Bytes.BYTES_RAWCOMPARATOR.compare(ent.getValue().getGroup(),entry.getValue().getGroup()) == 0) {
				return true;
			}
		}
		
		return false;*/
	}
	
	public void add(Entry<byte[],RawScanResult> entry) {
		if(map.get(Base64.encodeBase64String(entry.getValue().getGroup())) != null) {
			map.get(Base64.encodeBase64String(entry.getValue().getGroup())).mergeServerSide(entry.getValue(), sim, SliceOperation.ADD);
		} else {
			set.add(entry);
			map.put(Base64.encodeBase64String(entry.getValue().getGroup()), entry.getValue());
		}
	}
	
	public void addAll(Collection<Entry<String,RawScanResult>> coll) {
		for(Entry<String,RawScanResult> en : coll) {
			add(new SimpleEntry<byte[],RawScanResult>(Base64.decodeBase64(en.getKey()),en.getValue()));
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
	
	/**
	 * returns all entries in the sorted tree
	 * 
	 * @return
	 */
	public Collection<Entry<String,RawScanResult>> getEntries() {
		return map.entrySet();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		comparator.write(out);
		
		out.writeInt(map.size());
		for(Entry<String,RawScanResult> entry : map.entrySet()) {
			out.writeInt(entry.getKey().getBytes().length);
			out.write(entry.getKey().getBytes());
			entry.getValue().write(out);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
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
