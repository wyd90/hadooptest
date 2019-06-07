package com.wyd.mr.jointest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapJoin {
    public static class MapJonMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        private Map<String,String> allCustomers = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(new Path(""));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null){
                String cid = line.substring(0, line.indexOf(","));
                allCustomers.put(cid,line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String cid = line.substring(line.lastIndexOf(",")+1);

            String orderInfo = line.substring(0,line.lastIndexOf(","));

            String customerInfo = allCustomers.get(cid);

            context.write(new Text(customerInfo+","+orderInfo),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("MapJoinApp");
        job.setJarByClass(MapJoin.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(0);

        job.setMapperClass(MapJonMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);
    }
}
