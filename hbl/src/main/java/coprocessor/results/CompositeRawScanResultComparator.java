package coprocessor.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.client.impl.scanner.RawScanResult;

/**
 * Composite of multiple comparators to handle the case of multiple order by statements, i.e. order by <dimension>,<aggregated measure>
 * 
 * @author michael
 *
 */
public class CompositeRawScanResultComparator implements RawScanResultComparatorInterface {
	
	private List<RawScanResultComparatorInterface> list = new ArrayList<RawScanResultComparatorInterface>();

	public CompositeRawScanResultComparator() {
		
	}
	
	public CompositeRawScanResultComparator(List<RawScanResultComparatorInterface> _list) {
		list = _list;
	}
	
	public void add(RawScanResultComparatorInterface r) {
		list.add(r);
	}
	
	public List<RawScanResultComparatorInterface> getComparators() {
		return list;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
	 */
	@Override
	public int compare(Entry<byte[], RawScanResult> o1, Entry<byte[], RawScanResult> o2) {
		int comparison;
		for(Comparator<Entry<byte[],RawScanResult>> comp : list) {
			comparison = comp.compare(o1, o2);
			if(comparison != 0) {
				return comparison;
			}
		}
		
		return Bytes.BYTES_RAWCOMPARATOR.compare(o1.getKey(),o2.getKey());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(list.size());
		for(int i=0;i<list.size();i++) {
			list.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int num = in.readInt();
		RawScanResultMeasureComparator im;
		for(int i=0;i<num;i++) {
			im = new RawScanResultMeasureComparator();
			im.readFields(in);
			list.add(im);
		}
	}

}
