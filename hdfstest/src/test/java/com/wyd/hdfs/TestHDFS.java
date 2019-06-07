package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class TestHDFS {

    /**
     * 读取/user/root/a.txt文件的内容
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testSave() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        FSDataInputStream in = fs.open(new Path("a.txt"));

        IOUtils.copyBytes(in, System.out, 4096, true);

        IOUtils.closeStream(in);


    }

    /**
     *向hdfs中写入文件
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testWrite() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");


        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/wangyadi/Documents/安装程序/apache-storm-1.1.2.tar.gz"));
        FSDataOutputStream out = fs.create(new Path("apache-storm-1.1.2.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }


    /**
     *向hdfs中写入文件，定制副本数和block块大小
     * 设置1副本，block块为64M
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testWriteWithDIY() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node2:9000");
        FileSystem fs = FileSystem.get(URI.create("hdfs://node2:9000"), conf, "root");

        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/wangyadi/Documents/安装程序/apache-storm-1.1.2.tar.gz"));

        FSDataOutputStream out = fs.create(new Path("apache-storm-1.1.2.tar.gz"), true, 4096, new Short("1"), 67108864L);

        IOUtils.copyBytes(in, out, 4096, true);


    }


}
