package com.zebin.demo04_FlowAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<LongWritable,FlowBean,LongWritable,FlowBean> {
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Reducer<LongWritable, FlowBean, LongWritable, FlowBean>.Context context) throws IOException, InterruptedException {
        flowBean.setUpFlow(0L);
        flowBean.setDownFlow(0L);
        flowBean.setSumFlow(0L);
        for (FlowBean flow:values) {
            flowBean.setUpFlow(flowBean.getUpFlow()+ flow.getUpFlow());
            flowBean.setDownFlow(flowBean.getDownFlow()+ flow.getDownFlow());
            flowBean.setSumFlow(flowBean.getSumFlow()+ flow.getSumFlow());
        }
        context.write(key,this.flowBean);
    }
}
