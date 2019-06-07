package com.wyd.mr;

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

public class WCWithCounter {
    public static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for(String word : words){
                context.write(new Text(word),new IntWritable(1));
                context.getCounter("m","WordCountMapper.map").increment(1);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for(IntWritable v : values){
                i++;
            }
            context.write(key, new IntWritable(i));
            context.getCounter("r","WordCountReducer.reduce").increment(1);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WCWithCounter.WordCountMapper.class);
        job.setReducerClass(WCWithCounter.WordCountReducer.class);

        job.setJarByClass(WCWithCounter.class);
        FileInputFormat.addInputPath(job, new Path("/wc"));
        FileOutputFormat.setOutputPath(job, new Path("/wc20190528"));

        job.waitForCompletion(true);

    }
}
