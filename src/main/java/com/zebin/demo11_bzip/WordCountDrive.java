package com.zebin.demo11_bzip;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDrive {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            //map输出端压缩  reduce会解压缩
            configuration.setBoolean("mapreduce.map.output.compress",true);
            configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WordCountDrive.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputPaths(job,new Path("src/main/file/Sun.txt"));
            FileOutputFormat.setOutputPath(job,new Path("src/main/file/aaa"));
            //reduce端开启压缩，设置压缩方式
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);
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
