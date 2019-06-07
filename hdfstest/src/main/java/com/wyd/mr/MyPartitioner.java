package com.wyd.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

    public int getPartition(Text text, IntWritable intWritable, int i) {
        return 0;
    }
}
