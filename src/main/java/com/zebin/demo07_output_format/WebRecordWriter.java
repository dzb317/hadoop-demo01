package com.zebin.demo07_output_format;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

public class WebRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream fsOutWebZebin;
    private  FSDataOutputStream fsOutWebOther;

    public WebRecordWriter(TaskAttemptContext taskAttemptContext) {
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            fsOutWebZebin = fs.create(new Path("src/main/file/aaa/zebin.txt"));
            fsOutWebOther = fs.create(new Path("src/main/file/aaa/other.txt"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().contains("zebin")){
            fsOutWebZebin.writeBytes(text.toString()+"\n");
        }else {
            fsOutWebOther.writeBytes(text.toString()+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fsOutWebOther);
        IOUtils.closeStream(fsOutWebZebin);

    }
}
