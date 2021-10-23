package com.zebin.demo05_writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, LongWritable> {
    private LongWritable longWritable = new LongWritable();
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, LongWritable>.Context context) throws IOException, InterruptedException {
        String readLine = value.toString();
        String[] line = readLine.split("    ");
        Long number = Long.parseLong(line[1]);
        Long upFlow = Long.parseLong(line[4]);
        Long downFlow = Long.parseLong(line[5]);
        this.longWritable.set(number);
        this.flowBean.setUpFlow(upFlow);
        this.flowBean.setDownFlow(downFlow);
        this.flowBean.setSumFlow(upFlow + downFlow);
        context.write(this.flowBean, this.longWritable);
    }
}
