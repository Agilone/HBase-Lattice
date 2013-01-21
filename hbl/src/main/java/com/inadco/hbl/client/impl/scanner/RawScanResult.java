/*
 * 
 *  Copyright © 2010, 2011 Inadco, Inc. All rights reserved.
 *  
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *  
 *  
 */
package com.inadco.hbl.client.impl.scanner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.inadco.hbl.api.AggregateFunctionRegistry;
import com.inadco.hbl.client.impl.SliceOperation;
import com.inadco.hbl.protocodegen.Cells.Aggregation;

/**
 * Raw scan result, the stuff that is collected and aggregated from the hbase
 * but doesn't have enough brains to do proper evaluations to maintain all
 * actual end-user result contracts.
 * <P>
 * 
 * @author dmitriy
 * 
 */
public class RawScanResult implements Cloneable, Writable {

    private byte[]                group;
    private Aggregation.Builder[] measures;
    private SliceOperation        sliceOperation;

    public RawScanResult() {
    	
    }
    
    public RawScanResult(int groupKeyLen, int getMeasureQualifiers, SliceOperation so) {
    	super();
    	setGroup(new byte[groupKeyLen]);
    	setMeasures(new Aggregation.Builder[getMeasureQualifiers]);
    	sliceOperation = so;
    }
    
    public RawScanResult(ScanSpec ss) {
        super();
        setGroup(new byte[ss.getGroupKeyLen()]);
        setMeasures(new Aggregation.Builder[ss.getMeasureQualifiers().length]);
        sliceOperation = ss.getSliceOperation();

    }
    
    @Override
	public void write(DataOutput out) throws IOException {
    	out.writeInt(measures.length);
		for(Aggregation.Builder b : measures) {
			byte[] arr = b.build().toByteArray();
			out.writeInt(arr.length);
			out.write(arr);
		}
		out.writeInt(group.length);
		out.write(group);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		measures = new Aggregation.Builder[in.readInt()];
		for(int i=0;i<measures.length;i++) {
			byte[] arr = new byte[in.readInt()];
			in.readFully(arr);
			measures[i] = Aggregation.newBuilder();
			measures[i].mergeFrom(arr);
		}
		group = new byte[in.readInt()];
		in.readFully(group);
	}

    public byte[] getGroup() {
        return group;
    }

    public void setGroup(byte[] group) {
        this.group = group;
    }

    public Aggregation.Builder[] getMeasures() {
        return measures;
    }

    public void setMeasures(Aggregation.Builder[] measures) {
        this.measures = measures;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        RawScanResult result = (RawScanResult) super.clone();
        result.group = group.clone();
        // TODO
        // result.measures=
        throw new CloneNotSupportedException();
    }

    public void reset() {
        Arrays.fill(measures, null);
    }

    public void mergeMeasures(RawScanResult other, AggregateFunctionRegistry afr, SliceOperation so) {
        for (int i = 0; i < measures.length; i++) {
            if (other.measures[i] != null) {
                if (measures[i] == null)
                    measures[i] = other.measures[i];
                else {
                    afr.mergeAll(measures[i], other.measures[i].clone().build(), so);
                }
            }
        }
    }

    /**
     * to sort or sort-merge results by group
     * 
     * @author dmitriy
     * 
     */
    public static class GroupComparator implements Comparator<RawScanResult> {

        @Override
        public int compare(RawScanResult o1, RawScanResult o2) {
            return Bytes.BYTES_RAWCOMPARATOR.compare(o1.group, o2.group);
        }
    }

    public SliceOperation getSliceOperation() {
        return sliceOperation;
    }

    public void setSliceOperation(SliceOperation sliceOperation) {
        this.sliceOperation = sliceOperation;
    }

}
