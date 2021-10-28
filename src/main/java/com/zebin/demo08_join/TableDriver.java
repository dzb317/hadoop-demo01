package com.zebin.demo08_join;

import com.google.common.collect.Table;
import com.zebin.demo07_output_format.WebDriver;
import com.zebin.demo07_output_format.WebMapper;
import com.zebin.demo07_output_format.WebOutPutFormat;
import com.zebin.demo07_output_format.WebReducer;
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

public class TableDriver {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Collection collection = new ArrayList();
            Job job = Job.getInstance(conf);
            job.setJarByClass(TableDriver.class);
            job.setMapperClass(TableMapper.class);
            job.setReducerClass(TableReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TableBean.class);
            job.setOutputKeyClass(TableBean.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.setInputPaths(job,new Path("src/main/file/JionFile"));
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
