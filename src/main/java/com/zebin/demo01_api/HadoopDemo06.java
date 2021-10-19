package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

public class HadoopDemo06 {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),configuration,"root");
        fs.copyFromLocalFile(new Path("/opt/homebrew/Cellar/mysql"),new Path("/hadoop/"));
        fs.close();
    }
}
