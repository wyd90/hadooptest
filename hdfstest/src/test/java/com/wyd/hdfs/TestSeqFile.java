package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class TestSeqFile {

    @Test
    public void save() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");

        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("a.seq"), IntWritable.class, Text.class);

        //创建压缩的seqence文件，block压缩模式
        //SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("a.seq.gzip"), IntWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, new GzipCodec());
        writer.append(new IntWritable(1), new Text("hello world tom"));
        writer.append(new IntWritable(2), new Text("tom jack hello"));
        IOUtils.closeStream(writer);

    }

    @Test
    public void read() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");

        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("a.seq"), conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)){
            System.out.println(reader.getPosition()+" "+key.toString()+" "+value.toString());
        }

        IOUtils.closeStream(reader);
    }
}
