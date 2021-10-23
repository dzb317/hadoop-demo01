package com.zebin.demo06_partion_and_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<FlowBean,LongWritable,LongWritable, FlowBean> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(FlowBean key, Iterable<LongWritable> values, Reducer<FlowBean,LongWritable,LongWritable, FlowBean>.Context context) throws IOException, InterruptedException {
        flowBean.setUpFlow(0L);
        flowBean.setDownFlow(0L);
        flowBean.setSumFlow(0L);
        for (LongWritable longWritable :values) {
            context.write(longWritable,key);
        }
    }
}
