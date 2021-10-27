package com.zebin.demo09_cache_join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public class CacheMapper extends Mapper<LongWritable,Text,CacheBean, NullWritable> {
    CacheBean cacheBean = new CacheBean();
    Properties properties = new Properties();
    @Override
    protected void setup(Mapper<LongWritable, Text, CacheBean, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fS = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fS.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open,"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] sp = line.split("  ");
            properties.put(sp[0],sp[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CacheBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("  ");
        cacheBean.setNum(split[0]);
        cacheBean.setName(split[1]);
        cacheBean.setDornumber(split[2]);
        cacheBean.setDormitory((String) properties.get(split[2]));
        cacheBean.setFlag("");
        context.write(cacheBean,NullWritable.get());
    }
}
