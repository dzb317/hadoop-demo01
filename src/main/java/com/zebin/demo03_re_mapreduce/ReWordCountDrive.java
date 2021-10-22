package com.zebin.demo03_re_mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReWordCountDrive {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(ReWordCountDrive.class);
            job.setMapperClass(ReWordCountMapper.class);
            job.setReducerClass(ReWordCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            /*
            设置传输文件的格式和虚拟块大小
            默认TextFileInputFormat
            job.setInputFormatClass(CombineFileInputFormat.class);
            CombineFileInputFormat.setMaxInputSplitSize(job,1024*1024*128);
            */
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
