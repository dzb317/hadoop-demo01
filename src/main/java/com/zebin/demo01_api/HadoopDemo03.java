package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HadoopDemo03 {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://121.37.84.67:9000"),conf,"root");
        fileSystem.mkdirs(new Path("/input"));
        fileSystem.close();
    }
}
