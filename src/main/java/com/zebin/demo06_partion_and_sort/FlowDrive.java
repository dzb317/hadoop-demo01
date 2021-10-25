package com.zebin.demo06_partion_and_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDrive {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(FlowDrive.class);
            job.setMapperClass(FlowMapper.class);
            job.setReducerClass(FlowReduce.class);
            job.setMapOutputKeyClass(FlowBean.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(FlowBean.class);
            job.setPartitionerClass(FlowPartitioner.class);
            job.setNumReduceTasks(8);

            FileInputFormat.setInputPaths(job, new Path("src/main/resources/phone_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path("src/main/resources/aaa"));
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}