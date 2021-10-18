package com.zebin.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class HadoopDemo02 {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs= FileSystem.get(new URI("hdfs://121.37.67.252:8020"),configuration,"root");




    }
}
