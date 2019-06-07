package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestMapFile {

    @Test
    public void save() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");

        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        MapFile.Writer writer = new MapFile.Writer(conf, fs,"hdfs://node2:9000/aMap", IntWritable.class, Text.class);

        writer.append(new IntWritable(1),new Text("aaa"));
        writer.append(new IntWritable(2), new Text("bbb"));

        IOUtils.closeStream(writer);

    }

    @Test
    public void read() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");
        conf.set("fs.defaultFS","hdfs://node2:9000");
        MapFile.Reader reader = new MapFile.Reader(fs,"hdfs://node2:9000/aMap", conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)){
            System.out.println(key.toString()+" "+value.toString());
        }
        IOUtils.closeStream(reader);
    }
}
