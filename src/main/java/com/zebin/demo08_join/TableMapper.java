package com.zebin.demo08_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text,TableBean> {

    private String name;
    private TableBean valueOut = new TableBean();
    private Text keyOut = new Text();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text,TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,TableBean>.Context context) throws IOException, InterruptedException {
        String valueIn = value.toString();
        String[] val = valueIn.split("  ");
        if (name.contains("a.txt")){
            valueOut.setNum(val[0]);
            valueOut.setName(val[1]);
            valueOut.setDornumber(val[2]);
            valueOut.setDormitory("");
            valueOut.setFlag("a");
        }else {
            valueOut.setNum("");
            valueOut.setName("");
            valueOut.setDornumber(val[0]);;
            valueOut.setDormitory(val[1]);
            valueOut.setFlag("b");
        }
        keyOut.set(valueOut.getDornumber());
        context.write(keyOut, valueOut);
    }
}
