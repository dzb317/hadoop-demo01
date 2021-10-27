package com.zebin.demo10_my_input_format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration;
    private FileSplit fileSplit;
    private boolean flag = false;
    private BytesWritable bytesWritable;
    private FSDataInputStream open;
    private FileSystem fileSystem;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!flag){
            fileSystem = FileSystem.get(configuration);
            open = fileSystem.open(fileSplit.getPath());
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(open, bytes, 0, bytes.length);
            bytesWritable = new BytesWritable();
            bytesWritable.set(bytes,0,bytes.length);
            flag = true;
            return flag;
        }
        return false;

    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        open.close();
        fileSystem.close();
    }
}
