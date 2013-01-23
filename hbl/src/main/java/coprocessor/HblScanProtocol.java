package coprocessor;
import java.io.IOException;


import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;


import com.inadco.hbl.api.Range;

import coprocessor.results.CompositeRawScanResultComparator;
import coprocessor.results.RawScanResultTree;



/**
 * Interface used by scanning coprocessor, specifying the protocol
 * 
 * @author michael
 *
 */
public interface HblScanProtocol extends CoprocessorProtocol {	
	RawScanResultTree getTopRowsMeasure(Range[] ranges, byte[][] measureQualifiers, int groupKeyLen, int[][] keyOffset, int rows, CompositeRawScanResultComparator rsrc, byte[] splitStartKey,
			final byte[] splitEndKey) throws IOException;
}