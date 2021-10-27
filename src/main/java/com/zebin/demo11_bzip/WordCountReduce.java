package com.zebin.demo11_bzip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReduce extends Reducer<Text, IntWritable, Text,IntWritable> {
    IntWritable OutValue= new IntWritable();
    @Override
    protected void reduce (Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i:values) {
            sum+=i.get();
        }
        OutValue.set(sum);
        context.write(key,OutValue);
    }
}
