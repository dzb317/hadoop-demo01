package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

public class HadoopDemo06 {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.84.67:9000"),configuration,"root");
        fs.copyFromLocalFile(new Path("/Users/dingzebin/Desktop/Code/Idea-code/hadoop/target/hadoop-1.0-SNAPSHOT.jar"),new Path("/hadoop/"));
        fs.close();
    }
}
