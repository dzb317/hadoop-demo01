package com.zebin.demo08_join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    private TableBean valOut = new TableBean();
    private NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        for (TableBean val: values) {
            if(val.getFlag().equals("a")){
                valOut.setNum(val.getNum());
                valOut.setName(val.getName());
                valOut.setDornumber(val.getDornumber());
            }else {
                valOut.setDormitory(val.getDormitory());

            }
        }
        context.write(valOut,nullWritable);
    }
}
