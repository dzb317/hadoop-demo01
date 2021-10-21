package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.net.URI;

public class HadoopDemo04 {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),configuration,"root");
        fs.rename(new Path("/test"),new Path("/hadoop"));
        fs.rename(new Path("/hadoop"),new Path("/input/Sun.txt"));
        fs.close();
    }
}
