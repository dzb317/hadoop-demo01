package com.zebin.demo07_output_format;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;


public class WebDriver {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Collection collection = new ArrayList();
            Job job = Job.getInstance(conf);
            job.setJarByClass(WebDriver.class);
            job.setMapperClass(WebMapper.class);
            job.setReducerClass(WebReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(WebOutPutFormat.class);
            FileInputFormat.setInputPaths(job,new Path("src/main/file/web.txt"));
            job.setOutputFormatClass(WebOutPutFormat.class);
            FileOutputFormat.setOutputPath(job,new Path("src/main/file/aaa"));
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
