package coprocessor.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import com.inadco.hbl.client.impl.scanner.RawScanResult;
import com.inadco.hbl.model.SimpleAggregateFunctionRegistry;

import coprocessor.ByteArrayComparator;

/**
 * Comparator to compare dimensions or measures with ascending or descending order.
 * 
 * @author michael
 *
 */
public class RawScanResultMeasureComparator implements RawScanResultComparatorInterface {
	
	
	private boolean isMeasure;
	private boolean isAsc;
	private int measureIndex;
	private String measureFunction;
	private int start;
	private int stop;
	
	private transient byte[] comp1;
	private transient byte[] comp2;
	private transient ByteArrayComparator asc;
	private static transient SimpleAggregateFunctionRegistry sim = new SimpleAggregateFunctionRegistry();
	
	public RawScanResultMeasureComparator() {
		
	}
	
	public RawScanResultMeasureComparator(int _measureIndex, String _measureFunction, boolean _isAsc) {
		isMeasure = true;
		start = 0;
		stop = 0;
		isAsc = _isAsc;
		measureIndex = _measureIndex;
		measureFunction = _measureFunction;
	}
	
	public RawScanResultMeasureComparator(int _start, int _stop, boolean _isAsc) {
		isMeasure = false;
		start = _start;
		stop = _stop;
		isAsc = _isAsc;
		measureIndex = 0;
		measureFunction = "";
		
		asc = new ByteArrayComparator(isAsc);
		comp1 = new byte[stop-start];
		comp2 = new byte[stop-start];
		
	}
	
	public String toString() {
		return "RawScanResultMeasureComparator: isMeasure:"+isMeasure+", start:"+start+", stop:"+stop+", isAsc:"+isAsc+", measureIndex:"+measureIndex+", measureFunction:"+measureFunction;
	}

	@Override
	public int compare(Entry<byte[], RawScanResult> o1, Entry<byte[], RawScanResult> o2) {
		if(isMeasure) {
			double d = (((Double.parseDouble(""+sim.findFunction(measureFunction).getAggrValue(o1.getValue().getMeasures()[measureIndex].build()))) - (Double.parseDouble(""+sim.findFunction(measureFunction).getAggrValue(o2.getValue().getMeasures()[measureIndex].build())))));
			if(isAsc) {
				if(d == 0.0) {
					return 0;
				} else if(d < 0.0) {
					return -1;
				}
				return 1;
			} else {
				if(d == 0.0) {
					return 0;
				} else if(d < 0.0) {
					return 1;
				}
				return -1;
			}
		} else {
			System.arraycopy(o1.getKey(), start, comp1, 0, comp1.length);
			System.arraycopy(o2.getKey(), start, comp2, 0, comp2.length);
			return asc.compare(comp1, comp2);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isMeasure);
		out.writeBoolean(isAsc);
		out.writeInt(measureIndex);
		
		out.writeInt(start);
		out.writeInt(stop);
		out.writeUTF(measureFunction);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isMeasure = in.readBoolean();
		isAsc = in.readBoolean();
		measureIndex = in.readInt();
		
		start = in.readInt();
		stop = in.readInt();
		measureFunction = in.readUTF();
		
		asc = new ByteArrayComparator(isAsc);
		
		if(!isMeasure) {
			comp1 = new byte[stop-start];
			comp2 = new byte[stop-start];
		}
	}

}
