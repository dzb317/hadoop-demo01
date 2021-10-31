package com.zebin.demo02_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDrive {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WordCountDrive.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            boolean result = job.waitForCompletion(true);
            System.exit(result?0:1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
