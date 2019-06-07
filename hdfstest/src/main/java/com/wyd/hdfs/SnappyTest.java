package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.IOException;

public class SnappyTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        Class clazz = SnappyCodec.class;
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://node2:9000");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream out = fs.create(new Path("access."+codec.getDefaultExtension()));

        CompressionOutputStream zipOut = codec.createOutputStream(out);

        IOUtils.copyBytes(new FileInputStream("/root/access.log"), zipOut, 4096, true);
    }
}
