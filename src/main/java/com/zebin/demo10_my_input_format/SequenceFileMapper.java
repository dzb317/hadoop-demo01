package com.zebin.demo10_my_input_format;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {

    @Override
    protected void map(NullWritable key, BytesWritable value, Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        context.write(new Text(fileName),value);
    }
}
