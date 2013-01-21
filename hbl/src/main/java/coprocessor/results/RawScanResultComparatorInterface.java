package coprocessor.results;

import java.util.Comparator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.inadco.hbl.client.impl.scanner.RawScanResult;

/**
 * Interface to compare 2 RawScanResults, entry contains RawScanResults and the hbase row in byte[] format
 * 
 * @author michael
 *
 */
public interface RawScanResultComparatorInterface extends Comparator<Entry<byte[],RawScanResult>>, Writable {

}
