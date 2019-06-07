package com.wyd.mr.temp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class MaxTemp {
    public static class MaxTempMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MaxTempReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for(IntWritable v : values){
                if(max < v.get()){
                    max = v.get();
                }
            }
            context.write(key, new IntWritable(max));
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTemp");

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setJarByClass(MaxTemp.class);
        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path("/Users/wangyadi/yarnData/mr/input/a.seq"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/wangyadi/yarnData/mr/output"));

        //设置全排序分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(conf, new Path("file:///Users/wangyadi/yarnData/mr/par/par.lst"));

        //创建随机采样器对象
        //freq每个key被选中的概率
        //numSamplesc抽取样本的总数
        //maxSplitsSampled划分的分区数
        InputSampler.RandomSampler<IntWritable, IntWritable> sampler = new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1, 1000, 3);

        //写入分区文件
        InputSampler.writePartitionFile(job, sampler);





        job.waitForCompletion(true);
    }
}
