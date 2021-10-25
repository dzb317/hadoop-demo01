package com.zebin.demo07_output_format;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class WebMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private NullWritable valueOut = NullWritable.get();
    private Text keyOut = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String web = value.toString();
        ArrayList a = new ArrayList<>();
        a.add(new Object());
        keyOut.set(web);
        context.write(keyOut,valueOut);
    }
}
