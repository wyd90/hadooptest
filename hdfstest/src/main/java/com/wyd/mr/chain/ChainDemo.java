package com.wyd.mr.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ChainDemo {

    public static class WcMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{

        private Text keyOut;

        private IntWritable vOut;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            keyOut = new Text();
            vOut = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split(" ");
            for(String k:arr){
                keyOut.set(k);
                context.write(keyOut, vOut);
            }
        }
    }

    public static class WcMapper2 extends Mapper<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String word = key.toString();
            if(!"iis".equals(word)){
                context.write(key, value);
            }
        }
    }

    public static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable value: values){
                count = count + value.get();
            }
            context.write(key, new IntWritable(count));

        }
    }

    public static class WcReducerMapper1 extends Mapper<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            int num = value.get();
            if(num > 5){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("chainDemo");
        job.setJarByClass(ChainDemo.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));


        ChainMapper.addMapper(job,WcMapper1.class,LongWritable.class,Text.class,Text.class,IntWritable.class,conf);
        ChainMapper.addMapper(job,WcMapper2.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);

        ChainReducer.setReducer(job,WcReducer.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);
        ChainReducer.addMapper(job, WcReducerMapper1.class, Text.class,IntWritable.class, Text.class, IntWritable.class,conf);

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);
    }

}
