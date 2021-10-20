package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class HadoopDemo07 {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),conf,"root");
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hadoop/mysql/8.0.26/"));
        for (FileStatus status:fileStatuses) {
            if(status.isFile()){
                System.out.println("文件："+status.getPath().getName());
            }else {
                System.out.println("目录："+status.getPath().getName());
            }

        }

        fileSystem.close();
    }
}
