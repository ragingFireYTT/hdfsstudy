package com.ytt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by ytt on 2018/12/8.
 */
public class hdfs {
    public static void main(String[] args) throws IOException {
        System.out.println();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop102:9000");
        final FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path("/ytt22"));
        fileSystem.close();
    }

    // 文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");
        fileSystem.copyFromLocalFile(new Path("d://test.txt"), new Path("/test.txt.txt"));
        fileSystem.close();
    }

    // 文件下载
    @Test
    public void testCoypToLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");

        fileSystem.copyToLocalFile(new Path("/test.txt"), new Path("/xx.txt"));
        fileSystem.close();
    }

    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("host = " + host);
                }
            }
            System.out.println("-----------------------------分割线---------------------");
        }

    }

    @Test
    public void testIsFile() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");
        FileStatus[] listFiles = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : listFiles) {
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fileSystem.close();
    }

    @Test
    public void testPutFile() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取对象。
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");
        //2. 输入流
        FileInputStream inputStream = new FileInputStream(new File("d://test.txt"));
        //3. 输出流
        FSDataOutputStream outputStream = fs.create(new Path("/stream5.xx"));

        byte[] b = new byte[1024];
        int i = inputStream.read(b);
        while (i >= 0) {
            outputStream.write(b, 0, i);
            i = inputStream.read(b);
        }

        IOUtils.copyBytes(inputStream,outputStream,conf);
        //4.关闭资源
//        IOUtils.closeStream(outputStream);
//        IOUtils.closeStream(inputStream);
        inputStream.close();
        inputStream = null;

        outputStream.close();
        outputStream = null;
        fs.close();
    }

    @Test
    public void testReadBlock1() throws Exception {
        //1:获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "ytt");
        //2:获取输入流
        FSDataInputStream inputStream = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3:获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("d:1.xx"));
        //4:拷贝
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            inputStream.read(bytes);
            outputStream.write(bytes);
        }
        //5:关闭资源
        inputStream.close();
        outputStream.close();
        fs.close();
    }

}
