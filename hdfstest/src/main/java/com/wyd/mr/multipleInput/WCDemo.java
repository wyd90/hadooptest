package com.wyd.mr.multipleInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCDemo {

    public static class WordCountsMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for(String word : words){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class WordCountsSeqMapper extends Mapper<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class WordCountsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for(IntWritable v : values){
                i++;
            }
            context.write(key, new IntWritable(i));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("multipleInputs");
        job.setJarByClass(WCDemo.class);

        MultipleInputs.addInputPath(job, new Path("file:///mr/seq"), SequenceFileInputFormat.class,WordCountsSeqMapper.class);
        MultipleInputs.addInputPath(job, new Path("file:///mr/txt"), TextInputFormat.class, WordCountsMapper.class);

        FileOutputFormat.setOutputPath(job, new Path("file:///mr/out"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountsReducer.class);

        job.waitForCompletion(true);

    }


}
