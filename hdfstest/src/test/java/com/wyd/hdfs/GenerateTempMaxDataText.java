package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.net.URI;

public class GenerateTempMaxDataText {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");

        FileSystem fs = FileSystem.get(URI.create("file:///"), conf, "root");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/Users/wangyadi/yarnData/mr/input/a.seq"), conf);

        IntWritable key = new IntWritable();
        IntWritable value = new IntWritable();

        while (reader.next(key, value)){
            if(key.get() == 1970){
                System.out.println(key.get() + " " + value.get());
            }

        }

        reader.close();

    }
}
