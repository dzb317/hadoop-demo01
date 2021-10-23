package com.zebin.demo06_partion_and_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<FlowBean, LongWritable> {

    @Override
    public int getPartition(FlowBean flowBean, LongWritable number, int i) {
        if(number.toString().substring(0,3).equals("130")){
            return 0;
        }else if(number.toString().substring(0,3).equals("131")){
            return 1;
        }else if(number.toString().substring(0,3).equals("132")){
            return 2;
        }else if(number.toString().substring(0,3).equals("133")){
            return 3;
        }else if(number.toString().substring(0,3).equals("134")){
            return 4;
        }else if(number.toString().substring(0,3).equals("135")){
            return 5;
        }else if(number.toString().substring(0,3).equals("136")){
            return 6;
        }else {
            return 7;
        }
    }
}
