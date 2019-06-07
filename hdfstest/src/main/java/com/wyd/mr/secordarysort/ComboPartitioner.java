package com.wyd.mr.secordarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComboPartitioner extends Partitioner<ComboKey, NullWritable> {
    public int getPartition(ComboKey comboKey, NullWritable nullWritable, int i) {
        return (comboKey.getYear().hashCode() & Integer.MAX_VALUE) % i;
    }
}
