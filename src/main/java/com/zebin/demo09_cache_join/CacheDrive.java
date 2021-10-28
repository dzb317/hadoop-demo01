package com.zebin.demo09_cache_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CacheDrive {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(CacheDrive.class);
            job.setOutputKeyClass(CacheBean.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapperClass(CacheMapper.class);
            job.setNumReduceTasks(0);
            job.addCacheFile(new URI("file:///Users/dingzebin/Desktop/Code/Idea-code/hadoop/src/main/file/b.txt"));
            FileInputFormat.setInputPaths(job,new Path("src/main/file/JionFile/a.txt"));
            FileOutputFormat.setOutputPath(job,new Path("src/main/file/aaa"));
            boolean b = job.waitForCompletion(true);
            System.exit(b?0:1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
