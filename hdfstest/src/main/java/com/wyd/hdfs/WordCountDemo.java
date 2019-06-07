package com.wyd.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDemo {
    public static class WordCountsMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for(String word : words){
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class WordCountsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountsMapper.class);
        job.setReducerClass(WordCountsReducer.class);

        job.setJarByClass(WordCountDemo.class);
        FileInputFormat.addInputPath(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        job.waitForCompletion(true);

    }
}
