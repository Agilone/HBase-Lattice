package coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;


import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.inadco.hbl.api.Range;
import com.inadco.hbl.client.HblAdmin;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.client.impl.scanner.CompositeKeyRowFilter;
import com.inadco.hbl.client.impl.scanner.RawScanResult;
import com.inadco.hbl.model.SimpleAggregateFunctionRegistry;
import com.inadco.hbl.protocodegen.Cells.Aggregation;
import com.inadco.hbl.util.HblUtil;

import coprocessor.results.CompositeRawScanResultComparator;
import coprocessor.results.RawScanResultTree;


/**
 * Coprocessor endpoint
 * 
 * @author michael
 *
 */
public class HblScanEndpoint extends BaseEndpointCoprocessor implements HblScanProtocol {

	public RawScanResultTree getTopRowsMeasure(Range[] ranges, byte[][] measureQualifiers, int groupKeyLen, int[][] keyOffset, int rows, CompositeRawScanResultComparator rsrc, byte[] splitStartKey,
			final byte[] splitEndKey) throws IOException {

		RawScanResultTree tree = new RawScanResultTree(rsrc);

		CompositeKeyRowFilter krf = new CompositeKeyRowFilter(ranges);
		byte[] startRow = krf.getCompositeBound(true);
		byte[] endRow = krf.getCompositeBound(false);
		if (HblUtil.incrementKey(endRow, 0, endRow.length))
			endRow = null;


		if (splitStartKey != null) {
			if (Bytes.compareTo(startRow, splitStartKey) < 0)
				startRow = splitStartKey;
			if (splitEndKey != null) {
				if (endRow == null)
					endRow = splitEndKey;
				else if (Bytes.compareTo(splitEndKey, endRow) < 0)
					endRow = splitEndKey;
			}

			if (endRow != null && Bytes.compareTo(endRow, startRow) < 0)
				endRow = startRow;
		}

		Scan scan = new Scan();
		scan.setCaching(5000);
		scan.setStartRow(startRow);
		if (endRow != null)
			scan.setStopRow(endRow);

		scan.setFilter(krf);

		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

		InternalScanner scanner = environment.getRegion().getScanner(scan);
		Result r;
		RawScanResult holder = null;
		RawScanResult prev_holder = null;

		Entry<byte[],RawScanResult> firstRawResult = null;
		Entry<byte[],RawScanResult> lastRawResult = null;

		SimpleAggregateFunctionRegistry sim = new SimpleAggregateFunctionRegistry();
		boolean firstResult = true;

		try {

			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				curVals.clear();
				done = scanner.next(curVals);

				r = new Result(curVals);

				if (holder == null) 
					holder = new RawScanResult(groupKeyLen,measureQualifiers.length, SliceOperation.ADD, keyOffset);

				byte[] row = r.getRow();

				
				//System.arraycopy(row, 0, holder.getGroup(), 0, groupKeyLen);
				holder.setCompositeGroup(row);
				
				int i = 0;
				for (byte[] measureQualifier : measureQualifiers) {

					KeyValue kv = r.getColumnLatest(HblAdmin.HBL_METRIC_FAMILY, measureQualifier);
					if (kv == null)
						holder.getMeasures()[i++] = null;
					else {
						Aggregation.Builder aggrB = Aggregation.newBuilder();

						aggrB.mergeFrom(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
						holder.getMeasures()[i++] = aggrB;
					}
				}

				if(firstResult) {
					firstRawResult = new SimpleEntry<byte[],RawScanResult>(row,holder);
					firstResult = false;
				} else {
					lastRawResult = new SimpleEntry<byte[],RawScanResult>(row,holder);
				}

				if(prev_holder != null) {

					if(Bytes.BYTES_RAWCOMPARATOR.compare(prev_holder.getGroup(), holder.getGroup()) == 0) {
						holder.mergeMeasuresSubgroups(prev_holder, sim, SliceOperation.ADD);
					} else {
						Entry<byte[],RawScanResult> dlowest = null;
						Entry<byte[],RawScanResult> current = new SimpleEntry<byte[],RawScanResult>(row,holder);

						dlowest = tree.last();

						if(firstResult) {
							firstResult = false;
						} else if(tree.size() < rows) {
							tree.add(current);
						} else if(dlowest != null) {
							if(rsrc.compare(current, dlowest) < 0) {
								tree.popLast();
								tree.add(current);
							}
						}
					}
				}

				prev_holder = holder;
				holder = new RawScanResult(groupKeyLen,measureQualifiers.length, SliceOperation.ADD, keyOffset);

			} while (done);

			if(lastRawResult != null && !tree.containsGroup(lastRawResult)) { tree.add(lastRawResult); }
			if(firstRawResult != null && !tree.containsGroup(firstRawResult)) { tree.add(firstRawResult); }

		} catch(Throwable t) {
			t.printStackTrace();
		} finally {
			scanner.close();
		}
		
		return tree;
	}
}
