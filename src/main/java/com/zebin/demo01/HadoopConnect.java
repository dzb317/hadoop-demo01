package com.zebin.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HadoopConnect {
    public static void main(String[] args) throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),new Configuration(),"root");
        fs.delete(new Path("/test"),true);
        fs.close();
    }

}
