package com.zebin.demo09_cache_join;

import com.zebin.demo08_join.TableBean;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CacheBean implements WritableComparable<CacheBean> {
    private String num;
    private String name;
    private String dormitory;
    private String dornumber;
    private String flag;

    public CacheBean() {
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDormitory() {
        return dormitory;
    }

    public void setDormitory(String dormitory) {
        this.dormitory = dormitory;
    }

    public String getDornumber() {
        return dornumber;
    }

    public void setDornumber(String dornumber) {
        this.dornumber = dornumber;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public int compareTo(CacheBean cacheBean) {
        return this.num.compareTo(cacheBean.getNum());
    }

    @Override
    public String toString() {
        return this.num +"\t"+ this.name +"\t"+ this.dormitory;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(num);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(dormitory);
        dataOutput.writeUTF(dornumber);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.num = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.dormitory = dataInput.readUTF();
        this.dornumber = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }
}
