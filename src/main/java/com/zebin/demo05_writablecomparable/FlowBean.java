package com.zebin.demo05_writablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public FlowBean(){
        super();
    }
    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.sumFlow);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readLong();
        this.downFlow=dataInput.readLong();
        this.sumFlow=dataInput.readLong();
    }

    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean flowBean){
        if(this.sumFlow > flowBean.getSumFlow()){
            return -1;
        }else if(this.sumFlow == flowBean.getSumFlow()){
            return 0;
        }else {
            return 1;
        }
    }
}
