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

    public int getMergedGroups() {
        return mergedGroups;
    }

    public void setMergedGroups(int mergedGroups) {
        this.mergedGroups = mergedGroups;
    }

    private byte[]                group;
    private Aggregation.Builder[] measures;
    private SliceOperation        sliceOperation;
    private int					  mergedGroups;
    private int[][]				  keyOffset;
    private byte[] trow;

    public RawScanResult() {

    }

    public RawScanResult(int groupKeyLen, int getMeasureQualifiers, SliceOperation so, int[][] keyOffset) {
        super();
        this.keyOffset = keyOffset;
        setGroup(new byte[groupKeyLen]);
        setMeasures(new Aggregation.Builder[getMeasureQualifiers]);
        sliceOperation = so;
        mergedGroups = 1;
    }

    public RawScanResult(ScanSpec ss) {
        super();
        setGroup(new byte[ss.getGroupKeyLen()]);
        setMeasures(new Aggregation.Builder[ss.getMeasureQualifiers().length]);
        sliceOperation = ss.getSliceOperation();
        mergedGroups = 0;
    }

    public void setCompositeGroup(byte[] row) { 
        if(keyOffset != null) {
            trow = row;
            int offset = 0;
            for(int i=0;i<(keyOffset[0].length-1);i++) {
                System.arraycopy(row, keyOffset[0][i], group, offset, keyOffset[1][i]);
                offset += keyOffset[1][i];
            }
        } else {
            System.arraycopy(row, 0, group, 0, group.length);
        }
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
        out.writeInt(mergedGroups);
        
        // System.out.println("mergedGroups write "+mergedGroups);
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
        mergedGroups = in.readInt();

        // System.out.println("mergedGroups "+mergedGroups);

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
        mergedGroups = 0;
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
        if(this.mergedGroups == 0) {
            this.mergedGroups += other.mergedGroups;
        }
        //this.mergedGroups += other.mergedGroups;
    }

    public void mergeMeasuresSubgroups(RawScanResult other, AggregateFunctionRegistry afr, SliceOperation so) {

        for (int i = 0; i < measures.length; i++) {
            if (other.measures[i] != null) {
                if (measures[i] == null) {
                    measures[i] = other.measures[i];
                } else {
                    afr.mergeAll(measures[i], other.measures[i].clone().build(), so);
                }
            }
        }
        if(keyOffset != null) {
            byte[] row1 = new byte[keyOffset[1][keyOffset[1].length-1]];
            byte[] row2 = new byte[keyOffset[1][keyOffset[1].length-1]];
            System.arraycopy(trow, keyOffset[0][keyOffset[0].length-1], row1, 0, row1.length);
            System.arraycopy(other.trow, keyOffset[0][keyOffset[0].length-1], row2, 0, row1.length);
            if(Bytes.BYTES_RAWCOMPARATOR.compare(row1, row2) != 0) {
                this.mergedGroups += other.mergedGroups;
            }
        }
    }

    public void mergeServerSide(RawScanResult other, AggregateFunctionRegistry afr, SliceOperation so) {
        for (int i = 0; i < measures.length; i++) {
            if (other.measures[i] != null) {
                if (measures[i] == null) {
                    measures[i] = other.measures[i];
                } else {
                    afr.mergeAll(measures[i], other.measures[i].clone().build(), so);
                }
            }
        }
        this.mergedGroups += other.mergedGroups;
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
