package com.wyd.mr.secordarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.util.Iterator;

public class MaxTempSecordarySort {

    public static class MaxTempSecordarySortMapper extends Mapper<IntWritable, IntWritable, ComboKey, NullWritable>{
        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            ComboKey comboKey = new ComboKey();
            comboKey.setYear(key.get());
            comboKey.setTemp(value.get());
            context.write(comboKey, NullWritable.get());
        }
    }

    public static class MaxTempSecordarySortReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable>{
        @Override
        protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<NullWritable> iterator = values.iterator();
            iterator.next();
            context.write(new IntWritable(key.getYear()), new IntWritable(key.getTemp()));

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("MaxTempSecordarySort");

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setJarByClass(MaxTempSecordarySort.class);
        job.setMapperClass(MaxTempSecordarySortMapper.class);
        job.setReducerClass(MaxTempSecordarySortReducer.class);

        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(ComboPartitioner.class);
        job.setGroupingComparatorClass(ComboGrouping.class);
        job.setSortComparatorClass(KeyComparator.class);

        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path("/Users/wangyadi/yarnData/mr/input/a.seq"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/wangyadi/yarnData/mr/output"));

//        //设置全排序分区类
//        job.setPartitionerClass(TotalOrderPartitioner.class);
//        TotalOrderPartitioner.setPartitionFile(conf, new Path("file:///Users/wangyadi/yarnData/mr/par/par.lst"));
//
//        //创建随机采样器对象
//        //freq每个key被选中的概率
//        //numSamplesc抽取样本的总数
//        //maxSplitsSampled划分的分区数
//        InputSampler.RandomSampler<IntWritable, IntWritable> sampler = new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1, 1000, 3);
//
//        //写入分区文件
//        InputSampler.writePartitionFile(job, sampler);


        job.waitForCompletion(true);
    }


}
