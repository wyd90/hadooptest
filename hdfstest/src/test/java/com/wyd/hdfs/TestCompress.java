package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class TestCompress {

    @Test
    public void deflateCompress() throws IOException, InterruptedException {
        Class clazz = DeflateCodec.class;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        FSDataOutputStream out = fs.create(new Path("a.delate"));

        CompressionOutputStream zipOut = codec.createOutputStream(out);

        IOUtils.copyBytes(new FileInputStream("/Users/wangyadi/Documents/a.xml"), zipOut, 4096, true);

    }

    private Class[] zipClasses = {
            DeflateCodec.class,
            GzipCodec.class,
            BZip2Codec.class
            //Lz4Codec.class
    };

    public void zip(Class codecClass) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        FSDataOutputStream out = fs.create(new Path("a."+codec.getDefaultExtension()));

        CompressionOutputStream zipOut = codec.createOutputStream(out);

        IOUtils.copyBytes(new FileInputStream("/Users/wangyadi/Documents/a.xml"), zipOut, 4096, true);
    }

    @Test
    public void otherCompress() throws IOException, InterruptedException {
        for(Class c : zipClasses){
            zip(c);
        }
    }

    @Test
    public void unzipDeflate() throws IOException, InterruptedException {
        Class clazz = DeflateCodec.class;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        FSDataInputStream in = fs.open(new Path("a.delate"));
        CompressionInputStream zipIn = codec.createInputStream(in);

        IOUtils.copyBytes(zipIn, new FileOutputStream("/Users/wangyadi/Documents/a.deflate.xml"),4096,true);
    }
}

















