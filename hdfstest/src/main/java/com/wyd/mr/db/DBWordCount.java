package com.wyd.mr.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import java.io.IOException;

public class DBWordCount {
    public static class DBWordCountMapper extends Mapper<LongWritable, MyDBWritable, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {
            String[] arr = value.getTxt().toString().split(" ");
            for (String str : arr){
                context.write(new Text(str), new IntWritable(1));
            }
        }
    }

    public static class DBWordCountReducer extends Reducer<Text, IntWritable, DBOutWritable, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable w : values){
                count = count + w.get();
            }
            DBOutWritable out = new DBOutWritable();
            out.setWord(key.toString());
            out.setCnts(count);

            context.write(out, NullWritable.get());
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("dbWordCount");
        job.setJarByClass(DBWordCount.class);

        DBConfiguration.configureDB(job.getConfiguration(), "com.mysql.jdbc.Driver", "jdbc:mysql://node3:3306/bigdata", "root", "az63091919");
        DBInputFormat.setInput(job, MyDBWritable.class, "select id,name,txt from words", "select count(*) from words");

        DBOutputFormat.setOutput(job, "wordcount", "word","cnts");

        job.setMapperClass(DBWordCountMapper.class);
        job.setReducerClass(DBWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MyDBWritable.class);

        job.waitForCompletion(true);
    }

}
