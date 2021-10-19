package com.zebin.demo01_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

public class HadoopDemo05 {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),configuration,"root");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"),true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println("===========");
            System.out.println(next.getPath());
            System.out.println(next.getLen());
            System.out.println(next.getOwner());
            System.out.println(next.getBlockSize());
            System.out.println(next.getPermission());
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        fs.close();
    }
}
