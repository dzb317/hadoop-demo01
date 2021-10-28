package com.zebin.demo01_api;

import java.io.FileInputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/*
 * 翻译：线程“main”中的异常org.apache.hadoop.ipc.RemoteException
 * （java.io.IOException）：文件/test222.xml只能复制到0个节点而不是minReplication（= 1）。
 * 运行中有1个数据节点，此操作中排除了1个节点。
 * 原因：
 * 如果您确定数据节点的容量没有用尽并且主节点和数据节点之间的通信运行良好，
 * 那么问题可能是由URL解析引起的。具体来说，HDFS客户端无法连接到datanode，尽管它可以与主节点连接。
 * 解决办法：
 * 1.在org.apache.hadoop.conf.Configuration实例上添加以下配置：
 * （1）  Configuration conf = new Configuration();
 *       conf.set("dfs.client.use.datanode.hostname", "true");
 *       conf.set("fs.defaultFS", "hdfs://hadoop-node-name-node:8020");//可不加
 * （2）  也可配置到resources文件夹中,新建hdfs-site.xml 添加以下内容
 *      <property>
 *      <name>dfs.client.use.datanode.hostname</name>
 *      <value>true</value>
 *      <description>only cofig in clients</description>
 *      </property>
 * （3） 该配置只能配置到客户端，不可配置到hadoop主机上
 * 2.确认您的客户端可以ping datanode
 * 如果ping失败，则手动添加解析映射。
 * （1）在Mac OS中，使用以下命令编辑主机文件。
 *    sudo vim /etc/hosts
 * （2）将以下映射添加到文件中。
 *    ip(公网IP) hadoop-node
 * */
public class HadoopDemo01 {
    public static void main(String[] args) throws Exception{
        Configuration conf =new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://121.37.67.252:8020"),conf,"root");
        FileInputStream input = new FileInputStream("src/main/file/Sun.txt");
        FSDataOutputStream output = fs.create(new Path("/sun.txt"));
        IOUtils.copyBytes(input, output, conf);
        IOUtils.closeStream(input);
        IOUtils.closeStream(output);
    }

}