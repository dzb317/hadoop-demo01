package com.zebin.demo07_output_format;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WebReducer extends Reducer<Text, NullWritable, Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        for (NullWritable valueIn:values) {
            context.write(key,valueIn);
        }
    }
}
